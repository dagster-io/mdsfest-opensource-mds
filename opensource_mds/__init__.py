from dagster import (
    Definitions,
    load_assets_from_modules,
    define_asset_job,
)

from . import assets, resources

all_assets = load_assets_from_modules([assets])
duckdb_resource = resources.duckdb_resource
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
        "sling": resources.sling_resource,
        "dbt": resources.dbt_resource,
    },
    jobs=[assets_job],
)
