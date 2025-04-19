from dataclasses import dataclass
from pathlib import Path
from typing import List

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable
from dagster_duckdb import DuckDBResource


@dataclass
class CSVToDuckDB(Component, Resolvable):
    """Loads a CSV file into a DuckDB table.

    Attributes:
        name (str): The name to assign to the generated Dagster asset and table.
        csv_file_path (str): The relative path to the CSV file from the project data directory.
        depends_on (str): The asset this component depends on for the CSV data.
        description (str): Optional description of the data being loaded.
    """
    name: str
    csv_file_path: str
    depends_on: str
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
            deps=[self.depends_on],
            compute_kind="duckdb",
            group_name="prepared",
            retry_policy=retry_policy,
        )
        def _asset(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
            csv_path = dg.file_relative_path(__file__, self.csv_file_path)
            with duckdb.get_connection() as conn:
                conn.execute(
                    f"""CREATE OR REPLACE TABLE {self.name} AS (
                            SELECT * FROM read_csv_auto('{csv_path}'))
                    """
                )
                nrows = conn.execute(f"SELECT COUNT(*) FROM {self.name}").fetchone()[0]  # type: ignore

                metadata = conn.execute(
                    f"select * from duckdb_tables() where table_name = '{self.name}'"
                ).pl()

            context.add_output_metadata(
                metadata={
                    "num_rows": nrows,
                    "table_name": metadata["table_name"][0],
                    "database_name": metadata["database_name"][0],
                    "schema_name": metadata["schema_name"][0],
                    "column_count": metadata["column_count"][0],
                    "estimated_size": metadata["estimated_size"][0],
                    "description": self.description,
                    "csv_file_path": self.csv_file_path,
                }
            )

            context.log.info(f"Created {self.name} table")

        return dg.Definitions(assets=[_asset]) 