import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_sling import (
    SlingResource,
    sling_assets,
)

from opensource_mds.defs.resources import CustomDagsterDbtTranslator, dbt_manifest_path

retry_policy = dg.RetryPolicy(
    max_retries=3,
    delay=0.2,  # 200ms
    backoff=dg.Backoff.EXPONENTIAL,
)


replication_config = {
    "source": "postgres",
    "target": "duckdb",
    "defaults": {"mode": "full-refresh", "object": "{stream_schema}_{stream_table}"},
    "streams": {"public.tickets": None, "public.events": None},
}


@sling_assets(replication_config=replication_config)
def sling_sync_assets(context, sling: SlingResource):
    yield from sling.replicate(context=context)
    for row in sling.stream_raw_logs():
        context.log.info(row)


@dbt_assets(
    manifest=dbt_manifest_path, dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def dbt_birds(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
