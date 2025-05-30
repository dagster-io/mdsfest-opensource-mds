from airflow.utils.task_group import TaskGroup
from airflow.sdk import Asset

from include.operators.download_extract_operator import DownloadAndExtractOperator
from include.operators.sling_operator import SlingPostgresToDuckDBOperator
from include.utils.constants import (
    CHECKLIST_2020_URL,
    CHECKLIST_2023_URL,
    SITE_DESCRIPTION_DATA_URL,
    SPECIES_TRANSLATION_DATA_URL,
    RAW_DATA_DIR,
    POSTGRES_CONFIG,
    DUCKDB_DATABASE
)


def create_raw_data_task_group(group_id: str = "raw_data_extraction", outlets: list = None) -> TaskGroup:
    """
    Creates a task group for raw data extraction tasks.
    
    This group contains all parallel tasks for downloading ZIP files
    and syncing PostgreSQL data to DuckDB.
    
    Args:
        group_id: ID for the task group
        outlets: List of assets that this task group produces
        
    Returns:
        TaskGroup containing all raw data extraction tasks
    """
    
    # Default asset outlets if none provided
    if outlets is None:
        outlets = [
            Asset("raw_checklist_2020_data"),
            Asset("raw_checklist_2023_data"),
            Asset("raw_site_data"),
            Asset("raw_species_translation_data"),
            Asset("raw_postgres_tickets_data"),
            Asset("raw_postgres_events_data"),
        ]
    
    with TaskGroup(group_id=group_id, tooltip="Extract raw data from various sources") as group:
        
        # Download checklist 2020 data
        DownloadAndExtractOperator(
            task_id="download_checklist_2020",
            source_url=CHECKLIST_2020_URL,
            extract_path="{{ var.value.data_dir }}/" + RAW_DATA_DIR,
            asset_name="checklist_2020",
            outlets=[Asset("raw_checklist_2020_data")],
            doc_md="""
            Downloads and extracts 2016-2020 bird checklist data from Cornell Lab.
            Produces raw_checklist_2020_data asset.
            """,
        )
        
        # Download checklist 2023 data  
        DownloadAndExtractOperator(
            task_id="download_checklist_2023",
            source_url=CHECKLIST_2023_URL,
            extract_path="{{ var.value.data_dir }}/" + RAW_DATA_DIR,
            asset_name="checklist_2023",
            outlets=[Asset("raw_checklist_2023_data")],
            doc_md="""
            Downloads and extracts 2021-2023 bird checklist data from Cornell Lab.
            Produces raw_checklist_2023_data asset.
            """,
        )
        
        # Download site description data
        DownloadAndExtractOperator(
            task_id="download_site_description_data",
            source_url=SITE_DESCRIPTION_DATA_URL,
            extract_path="{{ var.value.data_dir }}/" + RAW_DATA_DIR,
            asset_name="site_description_data",
            outlets=[Asset("raw_site_data")],
            doc_md="""
            Downloads and extracts bird count site description data.
            Produces raw_site_data asset.
            """,
        )
        
        # Download species translation data
        DownloadAndExtractOperator(
            task_id="download_species_translation_data",
            source_url=SPECIES_TRANSLATION_DATA_URL,
            extract_path="{{ var.value.data_dir }}/" + RAW_DATA_DIR,
            asset_name="species_translation_data",
            outlets=[Asset("raw_species_translation_data")],
            doc_md="""
            Downloads and extracts species translation table data.
            Produces raw_species_translation_data asset.
            """,
        )
        
        # Sync PostgreSQL data to DuckDB using Sling
        SlingPostgresToDuckDBOperator(
            task_id="sync_postgres_to_duckdb",
            postgres_config=POSTGRES_CONFIG,
            duckdb_database_path="{{ var.value.data_dir }}/" + DUCKDB_DATABASE,
            tables_to_sync=["public.tickets", "public.events"],
            outlets=[Asset("raw_postgres_tickets_data"), Asset("raw_postgres_events_data")],
            doc_md="""
            Syncs PostgreSQL tables (tickets and events) to DuckDB.
            Produces raw_postgres_tickets_data and raw_postgres_events_data assets.
            """,
        )
        
        # All tasks in this group run in parallel (no dependencies within the group)
        
    return group 