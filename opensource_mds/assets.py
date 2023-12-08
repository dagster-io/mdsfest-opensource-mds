import time
from datetime import datetime, timedelta
import zipfile
from tempfile import NamedTemporaryFile
from dagster_duckdb import DuckDBResource
from dagster_embedded_elt.sling import SlingMode, build_sling_asset
import pandas as pd
import requests
import subprocess
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    Backoff,
    RetryPolicy,
    file_relative_path,
    asset,
    OpExecutionContext,
)
from . import constants
from .resources import CustomDagsterDbtTranslator, dbt_manifest_path
from dagster_dbt import dbt_assets, DbtCliResource

from typing import List, Tuple

retry_policy = RetryPolicy(
    max_retries=3,
    delay=0.2,  # 200ms
    backoff=Backoff.EXPONENTIAL,
)


def download_and_extract_data(
    context: AssetExecutionContext, url: str
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
                file_relative_path(__file__, "../data/raw/checklist_data")
            )
            end_time = time.time()
            context.log.info(
                "Extracted checklist data to {}".format(
                    file_relative_path(__file__, "../raw/checklist_data")
                )
            )

        return extracted_names, end_time - start_time


@asset(compute_kind="python", group_name="raw_data")
def checklist_2020(context: AssetExecutionContext):
    extracted_names, elapsed_times = download_and_extract_data(
        context, constants.CHECKLIST_2020
    )
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(compute_kind="python", group_name="raw_data")
def checklist_2023(context: AssetExecutionContext):
    extracted_names, elapsed_times = download_and_extract_data(
        context, constants.CHECKLIST_2023
    )
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(compute_kind="python", group_name="raw_data")
def site_description_data(context: AssetExecutionContext):
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


@asset(compute_kind="python", group_name="raw_data")
def species_translation_data(context: AssetExecutionContext):
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


@asset(
    deps=[checklist_2020, checklist_2023],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def birds(context: AssetExecutionContext, duckdb: DuckDBResource):
    cl2020 = file_relative_path(__file__, constants.CL_2020_FPATH)
    cl2023 = file_relative_path(__file__, constants.CL_2023_FPATH)
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE birds_2020_tmp AS (
                    SELECT * FROM read_csv_auto('{cl2020}', sample_size=-1))
            """
        )
        conn.execute(
            f"CREATE OR REPLACE TABLE birds_2023_tmp AS (SELECT * FROM read_csv_auto('{cl2023}'))"
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

@asset
def fetch_usgs_data(context):
    api_url = "https://waterservices.usgs.gov/nwis/iv/"
    all_data = []

    for site in constants.USGS_SITES:
        params = {
            "format": "json",
            "sites": site,
            "parameterCd": constants.PARAMETER_CODE,  # Discharge, water temperature
        }

        response = requests.get(api_url, params=params)
        if response.status_code != 200:
            context.log.error(f"Failed to fetch data from USGS for site {site}")
            continue

        data = response.json()
        for series in data['value']['timeSeries']:
            # Extract the nested data
            for value_set in series['values']:
                df = pd.json_normalize(value_set['value'])
                df['site_number'] = site  # Add site number as a column
                df['site_name'] = series['sourceInfo']['siteName']  # Add site name
                all_data.append(df)

    # Concatenate all dataframes into a single dataframe
    combined_df = pd.concat(all_data, ignore_index=True)
    return combined_df


@asset
def process_usgs_data(context, fetch_usgs_data):
    context.log.info(f"Site info: {constants.SITE_INFO}") 
    df = fetch_usgs_data

    # Convert 'dateTime' to datetime and extract date
    df['dateTime'] = pd.to_datetime(df['dateTime'])
    df['date'] = df['dateTime'].dt.date

    # Ensure 'value' is numeric
    df['value'] = pd.to_numeric(df['value'], errors='coerce')

    # Group by date and site, then calculate mean
    daily_avg = df.groupby(['date', 'site_number']).mean().reset_index()

    # Add river and site names based on site_number
    daily_avg['river_name'] = daily_avg['site_number'].map(lambda x: constants.SITE_INFO.get(x, {}).get('river_name', 'Unknown'))
    daily_avg['site_name'] = daily_avg['site_number'].map(lambda x: constants.SITE_INFO.get(x, {}).get('site_name', 'Unknown'))

    return daily_avg

@asset(required_resource_keys={"duckdb"})
def load_data_to_duckdb(context: AssetExecutionContext, process_usgs_data):
    duckdb: DuckDBResource = context.resources.duckdb
    df = process_usgs_data  # Processed DataFrame from the previous asset

    # Define a path for the CSV file
    csv_file_path = '/Users/resford/Documents/GitHub/open-source-stack/data/raw/usgs_data.csv'  # Update this path as needed
    df.to_csv(csv_file_path, index=False)

    with duckdb.get_connection() as conn:
        try:
            # Load data from CSV into DuckDB
            conn.execute(
                f"""CREATE OR REPLACE TABLE usgs_data AS (
                        SELECT * FROM read_csv_auto('{csv_file_path}', sample_size=-1))
                """
            )
            nrows = conn.execute("SELECT COUNT(*) FROM usgs_data").fetchone()[0]

            # Fetching metadata and using tuple indices
            metadata_row = conn.execute(
                "SELECT * FROM duckdb_tables() WHERE table_name = 'usgs_data'"
            ).fetchone()

            if metadata_row:
                context.add_output_metadata(
                    metadata={
                        "num_rows": nrows,
                        "table_name": metadata_row[1],  # Adjust indices based on query structure
                        "database_name": metadata_row[0],
                        "schema_name": metadata_row[2],
                        "column_count": metadata_row[3],
                        "estimated_size": metadata_row[4],
                    }
                )

            context.log.info("Loaded data into usgs_data table")

            # Adding a CHECKPOINT
            conn.execute("CHECKPOINT")
            context.log.info("Checkpoint executed")

        except Exception as e:
            context.log.error(f"Error loading data into DuckDB: {e}")
            raise


@asset(
    deps=[species_translation_data],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def species(context: AssetExecutionContext, duckdb: DuckDBResource):
    species = file_relative_path(__file__, constants.SPECIES_TRANSLATION_FPATH)
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


@asset(
    deps=[site_description_data],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def sites(context: AssetExecutionContext, duckdb: DuckDBResource):
    sites = file_relative_path(__file__, constants.SITE_DATA_FPATH)
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


@asset(group_name="raw_data", compute_kind="steampipe")
def bird_toots_csv(context: AssetExecutionContext):
    result = subprocess.run(
        ["steampipe", "query", constants.STEAMPIPE_QUERY, "--output", "csv"],
        stdout=subprocess.PIPE,
    )
    output = result.stdout.decode().strip()
    toot_path = file_relative_path(__file__, "../data/raw/bird_toots.csv")
    with open(toot_path, "w") as file:
        file.write(output)

    context.log.info("Created bird_toots file")


@asset(
    deps=[bird_toots_csv],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def bird_toots(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(__file__, "../data/raw/bird_toots.csv")
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE bird_toots AS (
                    SELECT * FROM read_csv_auto('{fpath}'))
            """
        )
        nrows = conn.execute("SELECT COUNT(*) FROM bird_toots").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'bird_toots'"
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

    context.log.info("Created bird_toots table")


tickets_raw = build_sling_asset(
    AssetSpec(
        key=["tickets_raw"],
        group_name="raw_data",
    ),
    source_stream="tickets",
    target_object="tickets_raw",
    mode=SlingMode.FULL_REFRESH,
)

events_raw = build_sling_asset(
    AssetSpec(key=["events_raw"], group_name="raw_data"),
    source_stream="events",
    target_object="events_raw",
    mode=SlingMode.FULL_REFRESH,
)


@asset(
    deps=[tickets_raw],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def tickets(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(__file__, "../data/raw/tickets.csv")
    with duckdb.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE tickets AS (SELECT * FROM tickets_raw)")
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


@asset(
    deps=[events_raw],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def events(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(__file__, "../data/raw/events.csv")
    with duckdb.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE events AS (SELECT * FROM events_raw)")
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
def dbt_birds(context: OpExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
