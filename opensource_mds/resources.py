from dagster import file_relative_path, get_dagster_logger
from dagster import AssetKey, EnvVar
from dagster_dbt import DagsterDbtTranslator, DbtCliResource
from dagster_duckdb import DuckDBResource
from dagster_embedded_elt.sling import SlingResource, SlingSourceConnection, SlingTargetConnection

duckdb_database=file_relative_path(__file__, '/Users/resford/Documents/GitHub/open-source-stack/dbt_project/reports/osmds.db')

duckdb_resource = DuckDBResource(
    database=duckdb_database,
)

logger = get_dagster_logger()

PG_CONN_STR = "postgresql://demo:demo@sample-data.popsql.io:5432/marker"

sling_resource = SlingResource(
    source_connection=SlingSourceConnection(
        type="postgres", connection_string=PG_CONN_STR,
    ),
    target_connection=SlingTargetConnection(
        type="duckdb", instance=duckdb_database, duckdb_version="0.9.1",
    ),
)

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
