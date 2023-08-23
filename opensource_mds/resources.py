import os
import re
import sys

from dagster import file_relative_path, get_dagster_logger
from dagster import AssetKey, ConfigurableResource
from dagster_dbt import DagsterDbtTranslator, DbtCliResource
from dagster_duckdb import DuckDBResource
import sling

duckdb_resource = DuckDBResource(
    database=file_relative_path(__file__, "../data/db/osmds.db")
)
logger = get_dagster_logger()


class SlingResource(ConfigurableResource):
    postgres_connect_str: str

    def sync(
        self,
        source_table: str,
        destination_file: str,
        mode="full-refresh",
        primary_key=None,
    ):
        config = {
            "source": {
                "conn": self.postgres_connect_str,
                "stream": source_table,
                "primary_key": primary_key,
            },
            "target": {
                "object": f"file://{os.path.abspath(destination_file)}",
            },
            "mode": mode,
        }

        sling_cli = sling.Sling(**config)

        logger.info("Starting Sling sync with mode: %s", mode)
        messages = sling_cli.run(return_output=True, env={})

        pattern = r"(\d+) rows"
        num_rows = 0
        for line in messages.splitlines():
            sys.stdout.write(line + "\n")
            match = re.search(pattern, line)
            if match:
                num_rows = int(match.group(1))

        file_size = 0
        if os.path.isfile(destination_file):
            file_size = os.path.getsize(destination_file)
        else:
            logger.warning("Could not find file after writing")

        return file_size, num_rows


dbt_resource = DbtCliResource(
    project_dir=file_relative_path(__file__, "../dbt_project")
)
dbt_parse_invocation = dbt_resource.cli(["parse"], manifest={}).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props) -> AssetKey:
        return AssetKey(dbt_resource_props["name"])

    def get_group_name(cls, dbt_resource_props) -> str:
        return "prepared"
