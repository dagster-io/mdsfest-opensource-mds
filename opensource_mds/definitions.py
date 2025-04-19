import dagster as dg
import opensource_mds.defs as definitions
from opensource_mds.defs import resources
defs = dg.Definitions.merge(
    dg.components.load_defs(definitions),
    dg.Definitions(
        resources={
            "duckdb": resources.duckdb_resource,
            "sling": resources.sling_resource,
            "dbt": resources.dbt_resource,
        },
    ),
)