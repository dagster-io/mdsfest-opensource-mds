import time
import zipfile
from tempfile import NamedTemporaryFile
from typing import List, Tuple

import dagster as dg
from dagster_dbt import DbtCliResource, dbt_assets
import requests
from dagster_duckdb import DuckDBResource
from dagster_sling import (
    SlingResource,
    sling_assets,
)

from opensource_mds.defs.resources import CustomDagsterDbtTranslator, dbt_manifest_path

from . import constants

retry_policy = dg.RetryPolicy(
    max_retries=3,
    delay=0.2,  # 200ms
    backoff=dg.Backoff.EXPONENTIAL,
)


def download_and_extract_data(
    context: dg.AssetExecutionContext, url: str
) -> Tuple[List[str], float]:
    with NamedTemporaryFile(suffix=".zip") as f:
        start_time = time.time()
        context.log.info("Downloading checklist data from {}".format(url))
        r = requests.get(url)
        context.log.info("Downloaded {} bytes".format(len(r.content)))
        f.write(r.content)
        f.seek(0)

        with zipfile.ZipFile(f.name, "r") as zip_ref:
            extracted_names = zip_ref.namelist()
            zip_ref.extractall(
                dg.file_relative_path(__file__, "../data/raw/checklist_data")
            )
            end_time = time.time()
            context.log.info(
                "Extracted checklist data to {}".format(
                    dg.file_relative_path(__file__, "../raw/checklist_data")
                )
            )

        return extracted_names, end_time - start_time


@dg.asset(compute_kind="python", group_name="raw_data")
def site_description_data(context: dg.AssetExecutionContext):
    extracted_names, elapsed_times = download_and_extract_data(
        context, constants.SITE_DESCRIPTION_DATA
    )
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@dg.asset(compute_kind="python", group_name="raw_data")
def species_translation_data(context: dg.AssetExecutionContext):
    extracted_names, elapsed_times = download_and_extract_data(
        context, constants.SPECIES_TRANSLATION_DATA
    )
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@dg.asset(
    deps=[dg.AssetKey("checklist_2020"), dg.AssetKey("checklist_2023")],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def birds(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    cl2020 = dg.file_relative_path(__file__, constants.CL_2020_FPATH)
    cl2023 = dg.file_relative_path(__file__, constants.CL_2023_FPATH)
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE birds_2020_tmp AS (
                    SELECT * FROM read_csv_auto('{cl2020}', 
                        sample_size=-1))
            """
        )
        conn.execute(
            f"""CREATE OR REPLACE TABLE birds_2023_tmp AS (
                    SELECT * FROM read_csv_auto('{cl2023}',
                        sample_size=-1))
            """
        )
        conn.execute(
            """ CREATE OR REPLACE TABLE birds as (
                 SELECT * from birds_2020_tmp
                 UNION ALL
                 SELECT * from birds_2023_tmp
                )
            """
        )
        nrows = conn.execute("SELECT COUNT(*) FROM birds").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'birds'"
        ).pl()

    context.log.info("Created birds table")
    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )


@dg.asset(
    deps=[species_translation_data],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def species(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    species = dg.file_relative_path(__file__, constants.SPECIES_TRANSLATION_FPATH)
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE species AS (
                    SELECT * FROM read_csv_auto('{species}'))
            """
        )
        nrows = conn.execute("SELECT COUNT(*) FROM species").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'species'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created species table")


@dg.asset(
    deps=[site_description_data],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def sites(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    sites = dg.file_relative_path(__file__, constants.SITE_DATA_FPATH)
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE sites AS (
                    SELECT * FROM read_csv_auto('{sites}'))
            """
        )
        nrows = conn.execute("SELECT COUNT(*) FROM sites").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'sites'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created sites table")


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


@dg.asset(
    deps=[sling_sync_assets],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def tickets(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE OR REPLACE TABLE tickets AS (SELECT * FROM main.public_tickets)"
        )
        nrows = conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'tickets'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created tickets table")


@dg.asset(
    deps=[sling_sync_assets],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def events(context: dg.AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE OR REPLACE TABLE events AS (SELECT * FROM main.public_events)"
        )
        nrows = conn.execute("SELECT COUNT(*) FROM events ").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'events'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created events table")


@dbt_assets(
    manifest=dbt_manifest_path, dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def dbt_birds(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
