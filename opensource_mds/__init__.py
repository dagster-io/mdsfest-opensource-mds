from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
)

from . import assets, resources

all_assets = load_assets_from_modules([assets])
duckdb_resource = resources.duckdb_resource
sling_resource = resources.sling_resource
dbt_resource = resources.dbt_resource

assets_job = define_asset_job(
    name="etl_job",
    selection=all_assets,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 4,
                },
            }
        }
    },
)

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": duckdb_resource,
        "sling": sling_resource,
        "dbt": dbt_resource,
    },
    jobs=[assets_job],
)
