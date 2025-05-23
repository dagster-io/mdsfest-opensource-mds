from dagster import file_relative_path, get_dagster_logger
from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource
from dagster_duckdb import DuckDBResource
from dagster_sling import (
    SlingResource,
    SlingConnectionResource,
)

duckdb_database = file_relative_path(__file__, "../data/db/osmds.db")

duckdb_resource = DuckDBResource(
    database=duckdb_database,
)

logger = get_dagster_logger()

sling_resource = SlingResource(
    connections=[
        SlingConnectionResource(
            name="postgres",
            type="postgres",
            host="sample-data.popsql.io",
            user="demo",
            database="marker",
            password="demo",
        ),
        SlingConnectionResource(
            name="duckdb",
            type="duckdb",
            instance=duckdb_database,
        ),
    ]
)

dbt_resource = DbtCliResource(
    project_dir=file_relative_path(__file__, "../../dbt_project")
)
dbt_parse_invocation = dbt_resource.cli(["parse", "-q"], manifest={}).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props) -> AssetKey:
        return AssetKey(dbt_resource_props["name"])

    def get_group_name(cls, dbt_resource_props) -> str:
        return "prepared"
