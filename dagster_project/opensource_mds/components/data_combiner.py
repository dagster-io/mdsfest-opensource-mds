from dataclasses import dataclass
from typing import List

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable
from dagster_duckdb import DuckDBResource


@dataclass
class DataCombiner(Component, Resolvable):
    """Combines multiple CSV files into a single DuckDB table.

    Attributes:
        name (str): The name to assign to the generated Dagster asset and table.
        csv_file_paths (List[str]): List of relative paths to CSV files from the project data directory.
        depends_on (List[str]): List of assets this component depends on for the CSV data.
        description (str): Optional description of the combined data.
    """
    name: str
    csv_file_paths: List[str]
    depends_on: List[str]
    description: str = ""

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        # Define retry policy
        retry_policy = dg.RetryPolicy(
            max_retries=3,
            delay=0.2,  # 200ms
            backoff=dg.Backoff.EXPONENTIAL,
        )

        @dg.asset(
            name=self.name,
            deps=[dg.AssetKey(dep) for dep in self.depends_on],
            compute_kind="duckdb",
            group_name="prepared",
            retry_policy=retry_policy,
        )
        def _asset(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
            with duckdb.get_connection() as conn:
                # Create temporary tables for each CSV file
                for i, csv_path in enumerate(self.csv_file_paths):
                    full_csv_path = dg.file_relative_path(__file__, csv_path)
                    temp_table_name = f"{self.name}_{i}_tmp"
                    conn.execute(
                        f"""CREATE OR REPLACE TABLE {temp_table_name} AS (
                                SELECT * FROM read_csv_auto('{full_csv_path}', 
                                    sample_size=-1))
                        """
                    )

                # Combine all temporary tables with UNION ALL
                union_clauses = []
                for i in range(len(self.csv_file_paths)):
                    temp_table_name = f"{self.name}_{i}_tmp"
                    union_clauses.append(f"SELECT * from {temp_table_name}")

                union_query = " UNION ALL ".join(union_clauses)
                
                conn.execute(
                    f""" CREATE OR REPLACE TABLE {self.name} as (
                         {union_query}
                        )
                    """
                )
                
                nrows = conn.execute(f"SELECT COUNT(*) FROM {self.name}").fetchone()[0]  # type: ignore

                metadata = conn.execute(
                    f"select * from duckdb_tables() where table_name = '{self.name}'"
                ).pl()

            context.log.info(f"Created {self.name} table")
            context.add_output_metadata(
                metadata={
                    "num_rows": nrows,
                    "table_name": metadata["table_name"][0],
                    "database_name": metadata["database_name"][0],
                    "schema_name": metadata["schema_name"][0],
                    "column_count": metadata["column_count"][0],
                    "estimated_size": metadata["estimated_size"][0],
                    "description": self.description,
                    "csv_file_paths": self.csv_file_paths,
                    "num_source_files": len(self.csv_file_paths),
                }
            )

        return dg.Definitions(assets=[_asset]) 