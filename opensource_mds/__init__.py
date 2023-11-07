from dagster import Definitions, load_assets_from_modules

from . import assets, resources

all_assets = load_assets_from_modules([assets])
duckdb_resource = resources.duckdb_resource

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": duckdb_resource,
        "sling": resources.sling_resource,
        "dbt": resources.dbt_resource,
    },
)
