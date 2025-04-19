from dataclasses import dataclass
from typing import List, Union

import dagster as dg
from dagster.components import Component, ComponentLoadContext, Resolvable
from dagster_duckdb import DuckDBResource


@dataclass
class DuckDBTableCopier(Component, Resolvable):
    """Copies data from one DuckDB table to another.

    Attributes:
        name (str): The name to assign to the generated Dagster asset and target table.
        source_table (str): The name of the source table to copy from.
        depends_on (Union[str, List[str]]): The asset(s) this component depends on.
        description (str): Optional description of the data being copied.
    """
    name: str
    source_table: str
    depends_on: Union[str, List[str]]
    description: str = ""

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        # Define retry policy
        retry_policy = dg.RetryPolicy(
            max_retries=3,
            delay=0.2,  # 200ms
            backoff=dg.Backoff.EXPONENTIAL,
        )

        # Handle both single dependency and list of dependencies
        if isinstance(self.depends_on, str):
            deps = [self.depends_on]
        else:
            deps = self.depends_on

        @dg.asset(
            name=self.name,
            deps=deps,
            compute_kind="duckdb",
            group_name="prepared",
            retry_policy=retry_policy,
        )
        def _asset(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
            with duckdb.get_connection() as conn:
                conn.execute(
                    f"CREATE OR REPLACE TABLE {self.name} AS (SELECT * FROM {self.source_table})"
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
                    "source_table": self.source_table,
                }
            )

            context.log.info(f"Created {self.name} table")

        return dg.Definitions(assets=[_asset]) 