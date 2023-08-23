from dagster import Definitions, load_assets_from_modules

from . import assets, resources

all_assets = load_assets_from_modules([assets])
duckdb_resource = resources.duckdb_resource

# This sould be a EnvVar
PG_CONN_STR = "postgresql://demo:demo@sample-data.popsql.io:5432/marker"

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": duckdb_resource,
        "sling": resources.SlingResource(postgres_connect_str=PG_CONN_STR),
        "dbt": resources.dbt_resource,
    },
)
