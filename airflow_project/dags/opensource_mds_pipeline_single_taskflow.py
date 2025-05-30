import time
import zipfile
import tempfile
import subprocess
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

import requests

from airflow.sdk import dag, task, Asset, Metadata
from airflow.utils.task_group import TaskGroup

from include.utils.constants import (
    CL_2020_FPATH,
    CL_2023_FPATH,
    SITE_DATA_FPATH,
    SPECIES_TRANSLATION_FPATH,
    DUCKDB_DATABASE,
    CHECKLIST_2020_URL,
    CHECKLIST_2023_URL,
    SITE_DESCRIPTION_DATA_URL,
    SPECIES_TRANSLATION_DATA_URL,
    POSTGRES_CONFIG,
    RAW_DATA_DIR,
)

# ============================================================================
# ASSET DEFINITIONS
# ============================================================================

# Raw data assets
checklist_2020_asset = Asset(
    uri="file:///data/raw/checklist_2020_data", 
    name="checklist_2020_data",
    extra={"source": "cornell_lab", "years": "2016-2020", "type": "bird_observations"}
)

checklist_2023_asset = Asset(
    uri="file:///data/raw/checklist_2023_data", 
    name="checklist_2023_data",
    extra={"source": "cornell_lab", "years": "2021-2023", "type": "bird_observations"}
)

site_data_asset = Asset(
    uri="file:///data/raw/site_data", 
    name="site_data",
    extra={"source": "cornell_lab", "type": "site_descriptions"}
)

species_translation_asset = Asset(
    uri="file:///data/raw/species_translation_data", 
    name="species_translation_data",
    extra={"source": "cornell_lab", "type": "species_taxonomy"}
)

postgres_sync_asset = Asset(
    uri="duckdb:///data/db/postgres_tables", 
    name="postgres_sync_data",
    extra={"source": "postgresql", "tables": ["tickets", "events"]}
)

# Prepared table assets
birds_2020_table_asset = Asset(
    uri="duckdb:///data/db/birds_2020_tmp", 
    name="birds_2020_table",
    extra={"table_type": "staging", "source": "checklist_2020"}
)

birds_2023_table_asset = Asset(
    uri="duckdb:///data/db/birds_2023_tmp", 
    name="birds_2023_table",
    extra={"table_type": "staging", "source": "checklist_2023"}
)

species_table_asset = Asset(
    uri="duckdb:///data/db/species", 
    name="species_table",
    extra={"table_type": "reference", "source": "species_translation"}
)

sites_table_asset = Asset(
    uri="duckdb:///data/db/sites", 
    name="sites_table",
    extra={"table_type": "reference", "source": "site_data"}
)

tickets_table_asset = Asset(
    uri="duckdb:///data/db/tickets", 
    name="tickets_table",
    extra={"table_type": "operational", "source": "postgresql"}
)

events_table_asset = Asset(
    uri="duckdb:///data/db/events", 
    name="events_table",
    extra={"table_type": "operational", "source": "postgresql"}
)

birds_union_table_asset = Asset(
    uri="duckdb:///data/db/birds", 
    name="birds_union_table",
    extra={"table_type": "prepared", "source": "union_of_yearly_data"}
)

# dbt model assets
all_birds_model_asset = Asset(
    uri="duckdb:///data/db/all_birds", 
    name="all_birds_model",
    extra={"model_type": "dbt", "layer": "mart"}
)

top_birds_model_asset = Asset(
    uri="duckdb:///data/db/top_birds_by_year", 
    name="top_birds_by_year_model",
    extra={"model_type": "dbt", "layer": "mart", "aggregation": "yearly_summary"}
)

# Default DAG arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=2),
}

# ============================================================================
# SINGLE COMPLETE PIPELINE DAG
# ============================================================================

@dag(
    dag_id='opensource_mds_birds_pipeline_complete_taskflow',
    default_args=default_args,
    description='Open Source MDS Birds Data Pipeline - Complete Pipeline (TaskFlow with Assets)',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['birds', 'cornell-lab', 'mds', 'complete-pipeline', 'taskflow', 'assets'],
    doc_md="""
    # Open Source MDS Birds Data Pipeline - Complete Pipeline (TaskFlow API with Assets)
    
    This DAG contains the complete birds data pipeline using TaskFlow API with TaskGroups and Airflow Assets.
    
    ## Pipeline Stages:
    1. **Raw Data Extraction**: Downloads and extracts bird checklist data and syncs PostgreSQL data
    2. **Data Preparation**: Creates prepared DuckDB tables from raw data
    3. **dbt Transformations**: Runs dbt models to create analytics-ready data
    4. **Summary**: Final pipeline summary and notifications
    
    ## Data Flow:
    Raw Data → DuckDB Tables → dbt Models → Summary
    
    ## Assets:
    This pipeline defines and manages the following data assets:
    
    ### Raw Data Assets:
    - `checklist_2020_data`: Cornell Lab bird observations (2016-2020)
    - `checklist_2023_data`: Cornell Lab bird observations (2021-2023)
    - `site_data`: Site description data
    - `species_translation_data`: Species taxonomy data
    - `postgres_sync_data`: Synchronized PostgreSQL tables
    
    ### Prepared Table Assets:
    - `birds_2020_table`: Staging table for 2016-2020 data
    - `birds_2023_table`: Staging table for 2021-2023 data
    - `species_table`: Reference table for species data
    - `sites_table`: Reference table for site data
    - `tickets_table`: Operational tickets table
    - `events_table`: Operational events table
    - `birds_union_table`: Combined birds data from all years
    
    ### dbt Model Assets:
    - `all_birds_model`: Main analytical model
    - `top_birds_by_year_model`: Yearly aggregation model
    
    ## Asset-Aware Scheduling:
    Tasks automatically trigger based on asset updates, enabling efficient data lineage tracking
    and event-driven scheduling throughout the pipeline.
    """,
)
def complete_birds_pipeline():
    """
    Complete birds data pipeline using TaskFlow API.
    """
    
    # ========================================================================
    # RAW DATA EXTRACTION TASK GROUP
    # ========================================================================
    
    with TaskGroup("raw_data_extraction", tooltip="Download and extract raw data") as raw_data_group:
        
        @task(outlets=[checklist_2020_asset])
        def download_checklist_2020_data(data_dir: str) -> Dict[str, Any]:
            """
            Download and extract 2016-2020 checklist data.
            """
            return _download_and_extract_data(
                source_url=CHECKLIST_2020_URL,
                extract_path=f"{data_dir}/{RAW_DATA_DIR}",
                asset_name="checklist_2020_data"
            )
        
        @task(outlets=[checklist_2023_asset])
        def download_checklist_2023_data(data_dir: str) -> Dict[str, Any]:
            """
            Download and extract 2021-2023 checklist data.
            """
            return _download_and_extract_data(
                source_url=CHECKLIST_2023_URL,
                extract_path=f"{data_dir}/{RAW_DATA_DIR}",
                asset_name="checklist_2023_data"
            )
        
        @task(outlets=[site_data_asset])
        def download_site_data(data_dir: str) -> Dict[str, Any]:
            """
            Download and extract site description data.
            """
            return _download_and_extract_data(
                source_url=SITE_DESCRIPTION_DATA_URL,
                extract_path=f"{data_dir}/{RAW_DATA_DIR}",
                asset_name="site_data"
            )
        
        @task(outlets=[species_translation_asset])
        def download_species_translation_data(data_dir: str) -> Dict[str, Any]:
            """
            Download and extract species translation data.
            """
            return _download_and_extract_data(
                source_url=SPECIES_TRANSLATION_DATA_URL,
                extract_path=f"{data_dir}/{RAW_DATA_DIR}",
                asset_name="species_translation_data"
            )
        
        @task(outlets=[postgres_sync_asset])
        def sync_postgres_to_duckdb(data_dir: str) -> Dict[str, Any]:
            """
            Sync PostgreSQL tables (tickets and events) to DuckDB using Sling.
            """
            return _sync_postgres_to_duckdb(
                postgres_config=POSTGRES_CONFIG,
                duckdb_database_path=f"{data_dir}/{DUCKDB_DATABASE}",
                tables_to_sync=["public.tickets", "public.events"]
            )
        
        # Execute raw data tasks
        data_dir = "{{ var.value.data_dir }}"
        
        checklist_2020_result = download_checklist_2020_data(data_dir)
        checklist_2023_result = download_checklist_2023_data(data_dir)
        site_data_result = download_site_data(data_dir)
        species_data_result = download_species_translation_data(data_dir)
        postgres_sync_result = sync_postgres_to_duckdb(data_dir)
    
    # ========================================================================
    # DATA PREPARATION TASK GROUP
    # ========================================================================
    
    with TaskGroup("data_preparation", tooltip="Create prepared DuckDB tables") as data_prep_group:
        
        @task(max_active_tis_per_dag=1, pool='duckdb_pool', inlets=[checklist_2020_asset], outlets=[birds_2020_table_asset])
        def create_birds_2020_table(data_dir: str) -> Dict[str, Any]:
            """
            Create birds_2020_tmp table from 2016-2020 checklist CSV data.
            """
            return _create_duckdb_table_from_csv(
                database_path=f"{data_dir}/{DUCKDB_DATABASE}",
                table_name="birds_2020_tmp",
                csv_file_path=f"{data_dir}/{CL_2020_FPATH}"
            )
        
        @task(max_active_tis_per_dag=1, pool='duckdb_pool', inlets=[checklist_2023_asset], outlets=[birds_2023_table_asset])
        def create_birds_2023_table(data_dir: str) -> Dict[str, Any]:
            """
            Create birds_2023_tmp table from 2021-2023 checklist CSV data.
            """
            return _create_duckdb_table_from_csv(
                database_path=f"{data_dir}/{DUCKDB_DATABASE}",
                table_name="birds_2023_tmp",
                csv_file_path=f"{data_dir}/{CL_2023_FPATH}"
            )
        
        @task(max_active_tis_per_dag=1, pool='duckdb_pool', inlets=[species_translation_asset], outlets=[species_table_asset])
        def create_species_table(data_dir: str) -> Dict[str, Any]:
            """
            Create species table from species translation CSV data.
            """
            return _create_duckdb_table_from_csv(
                database_path=f"{data_dir}/{DUCKDB_DATABASE}",
                table_name="species",
                csv_file_path=f"{data_dir}/{SPECIES_TRANSLATION_FPATH}"
            )
        
        @task(max_active_tis_per_dag=1, pool='duckdb_pool', inlets=[site_data_asset], outlets=[sites_table_asset])
        def create_sites_table(data_dir: str) -> Dict[str, Any]:
            """
            Create sites table from site description CSV data.
            """
            return _create_duckdb_table_from_csv(
                database_path=f"{data_dir}/{DUCKDB_DATABASE}",
                table_name="sites",
                csv_file_path=f"{data_dir}/{SITE_DATA_FPATH}"
            )
        
        @task(max_active_tis_per_dag=1, pool='duckdb_pool', inlets=[postgres_sync_asset], outlets=[tickets_table_asset])
        def create_tickets_table(data_dir: str) -> Dict[str, Any]:
            """
            Create tickets table from PostgreSQL sync data.
            """
            return _create_duckdb_table_from_sql_with_check(
                database_path=f"{data_dir}/{DUCKDB_DATABASE}",
                table_name="tickets",
                source_table="public_tickets",
                fallback_query="CREATE OR REPLACE TABLE tickets AS (SELECT 'No data' as status, 0 as ticket_id, 'Placeholder' as description)"
            )
        
        @task(max_active_tis_per_dag=1, pool='duckdb_pool', inlets=[postgres_sync_asset], outlets=[events_table_asset])
        def create_events_table(data_dir: str) -> Dict[str, Any]:
            """
            Create events table from PostgreSQL sync data.
            """
            return _create_duckdb_table_from_sql_with_check(
                database_path=f"{data_dir}/{DUCKDB_DATABASE}",
                table_name="events",
                source_table="public_events",
                fallback_query="CREATE OR REPLACE TABLE events AS (SELECT 'No data' as status, 0 as event_id, 'Placeholder' as name)"
            )
        
        @task(max_active_tis_per_dag=1, pool='duckdb_pool', inlets=[birds_2020_table_asset, birds_2023_table_asset], outlets=[birds_union_table_asset])
        def create_birds_union_table(data_dir: str, birds_2020_metadata: Dict, birds_2023_metadata: Dict) -> Dict[str, Any]:
            """
            Create birds table by combining birds_2020_tmp and birds_2023_tmp.
            """
            return _create_duckdb_union_table(
                database_path=f"{data_dir}/{DUCKDB_DATABASE}",
                target_table="birds",
                source_tables=["birds_2020_tmp", "birds_2023_tmp"]
            )
        
        # Execute data preparation tasks with dependencies
        data_dir = "{{ var.value.data_dir }}"
        
        # Create individual tables in sequence to avoid DuckDB conflicts
        birds_2020_metadata = create_birds_2020_table(data_dir)
        birds_2023_metadata = create_birds_2023_table(data_dir)
        species_metadata = create_species_table(data_dir)
        sites_metadata = create_sites_table(data_dir)
        tickets_metadata = create_tickets_table(data_dir)
        events_metadata = create_events_table(data_dir)
        
        # Create union table (depends on birds tables)
        union_metadata = create_birds_union_table(data_dir, birds_2020_metadata, birds_2023_metadata)
        
        # Set explicit sequential dependencies for all DuckDB tasks
        birds_2020_metadata >> birds_2023_metadata >> species_metadata >> sites_metadata >> tickets_metadata >> events_metadata >> union_metadata
    
    # ========================================================================
    # DBT TRANSFORMATIONS TASK GROUP
    # ========================================================================
    
    with TaskGroup("dbt_transformations", tooltip="Run dbt models and transformations") as dbt_group:
        
        @task(inlets=[birds_union_table_asset, species_table_asset, sites_table_asset, tickets_table_asset, events_table_asset], outlets=[all_birds_model_asset])
        def dbt_run_all_birds(dbt_project_dir: str) -> Dict[str, Any]:
            """
            Build all primary dbt models excluding top_birds_by_year.
            """
            return _run_dbt_command(
                dbt_project_dir=dbt_project_dir,
                dbt_command="build",
                models=["all_birds", "daily_tickets", "stg_*"]
            )
        
        @task(inlets=[all_birds_model_asset], outlets=[top_birds_model_asset])
        def dbt_run_top_birds_by_year(dbt_project_dir: str, all_birds_metadata: Dict) -> Dict[str, Any]:
            """
            Run the top_birds_by_year model that depends on all other dbt models.
            """
            return _run_dbt_command(
                dbt_project_dir=dbt_project_dir,
                dbt_command="run",
                models=["top_birds_by_year"]
            )
        
        # Execute dbt transformations
        dbt_project_dir = "{{ var.value.dbt_project_dir }}"
        
        all_birds_metadata = dbt_run_all_birds(dbt_project_dir)
        top_birds_metadata = dbt_run_top_birds_by_year(dbt_project_dir, all_birds_metadata)
    
    # ========================================================================
    # SUMMARY TASK GROUP
    # ========================================================================
    
    with TaskGroup("pipeline_summary", tooltip="Generate pipeline summary and notifications") as summary_group:
        
        @task
        def generate_pipeline_summary() -> Dict[str, Any]:
            """
            Generate a summary of the completed pipeline run.
            """
            print("=== Open Source MDS Birds Pipeline Summary ===")
            print("✅ Raw data extraction completed")
            print("✅ Data preparation completed")
            print("✅ dbt transformations completed")
            print("✅ Pipeline execution successful!")
            
            summary = {
                "pipeline_status": "completed",
                "stages_completed": [
                    "raw_data_extraction",
                    "data_preparation", 
                    "dbt_transformations"
                ],
                "completion_time": datetime.now().isoformat()
            }
            
            return summary
        
        @task
        def send_completion_notification(summary: Dict[str, Any]) -> None:
            """
            Send pipeline completion notification (placeholder for actual notification logic).
            """
            print(f"📧 Pipeline completed successfully at {summary['completion_time']}")
            print("🔔 Notification sent to data team")
        
        # Execute summary tasks
        summary_result = generate_pipeline_summary()
        notification_result = send_completion_notification(summary_result)
    
    # ========================================================================
    # TASK GROUP DEPENDENCIES
    # ========================================================================
    
    # Set dependencies between task groups
    raw_data_group >> data_prep_group >> dbt_group >> summary_group

# ============================================================================
# UTILITY FUNCTIONS (converted from custom operators)
# ============================================================================

def _download_and_extract_data(source_url: str, extract_path: str, asset_name: str) -> Dict[str, Any]:
    """
    Download and extract ZIP file data.
    Converts DownloadAndExtractOperator functionality to a regular function.
    """
    # Create extract path
    extract_path_obj = Path(extract_path)
    extract_path_obj.mkdir(parents=True, exist_ok=True)
    
    with tempfile.NamedTemporaryFile(suffix=".zip") as f:
        start_time = time.time()
        
        # Download the file
        print(f"Downloading {asset_name} data from {source_url}")
        response = requests.get(source_url)
        print(f"Downloaded {len(response.content)} bytes")
        
        # Write to temporary file
        f.write(response.content)
        f.seek(0)
        
        # Extract the ZIP file
        with zipfile.ZipFile(f.name, "r") as zip_ref:
            extracted_names = zip_ref.namelist()
            zip_ref.extractall(extract_path_obj)
            end_time = time.time()
            print(f"Extracted {asset_name} data to {extract_path_obj}")
    
    elapsed_time = end_time - start_time
    
    # Create metadata dictionary
    metadata = {
        "names": extracted_names,
        "num_files": len(extracted_names),
        "elapsed_time": elapsed_time,
        "source_url": source_url,
        "asset_name": asset_name,
        "extract_path": str(extract_path_obj)
    }
    
    print(f"Extraction completed for {asset_name}:")
    print(f"  - Files extracted: {len(extracted_names)}")
    print(f"  - Elapsed time: {elapsed_time:.2f} seconds")
    print(f"  - Files: {extracted_names}")
    
    return metadata

def _create_duckdb_table_from_csv(database_path: str, table_name: str, csv_file_path: str) -> Dict[str, Any]:
    """
    Create DuckDB table from CSV file.
    Converts DuckDBCreateTableOperator CSV functionality to a regular function.
    """
    # Create database directory if needed
    db_path = Path(database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to DuckDB
    with duckdb.connect(database_path) as conn:
        print(f"Creating table {table_name} from CSV: {csv_file_path}")
        query = f"""
        CREATE OR REPLACE TABLE {table_name} AS (
            SELECT * FROM read_csv_auto('{csv_file_path}', sample_size=-1)
        )
        """
        
        # Execute the query
        conn.execute(query)
        
        # Get table metadata
        nrows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        # Get table information
        table_info_query = f"SELECT * FROM duckdb_tables() WHERE table_name = '{table_name}'"
        table_info = conn.execute(table_info_query).fetchone()
        
        if table_info:
            metadata = {
                "num_rows": nrows,
                "table_name": table_info[2],  # table_name
                "database_name": table_info[0],  # database_name
                "schema_name": table_info[1],  # schema_name
                "column_count": table_info[3] if len(table_info) > 3 else None,
                "estimated_size": table_info[4] if len(table_info) > 4 else None,
                "created_at": datetime.now().isoformat(),
                "source_file": csv_file_path
            }
        else:
            metadata = {
                "num_rows": nrows,
                "table_name": table_name,
                "database_name": "main",
                "schema_name": "main",
                "created_at": datetime.now().isoformat(),
                "source_file": csv_file_path
            }
    
    print(f"Table {table_name} created successfully:")
    print(f"  - Rows: {metadata['num_rows']}")
    print(f"  - Database: {metadata['database_name']}")
    print(f"  - Schema: {metadata['schema_name']}")
    
    return metadata

def _create_duckdb_table_from_sql(database_path: str, table_name: str, sql_query: str) -> Dict[str, Any]:
    """
    Create DuckDB table from SQL query.
    Converts DuckDBCreateTableOperator SQL functionality to a regular function.
    """
    with duckdb.connect(database_path) as conn:
        print(f"Creating table {table_name} with custom SQL")
        
        # Execute the query
        conn.execute(sql_query)
        
        # Get table metadata
        nrows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        
        # Get table information
        table_info_query = f"SELECT * FROM duckdb_tables() WHERE table_name = '{table_name}'"
        table_info = conn.execute(table_info_query).fetchone()
        
        if table_info:
            metadata = {
                "num_rows": nrows,
                "table_name": table_info[2],
                "database_name": table_info[0],
                "schema_name": table_info[1],
                "column_count": table_info[3] if len(table_info) > 3 else None,
                "estimated_size": table_info[4] if len(table_info) > 4 else None,
            }
        else:
            metadata = {
                "num_rows": nrows,
                "table_name": table_name,
                "database_name": "main",
                "schema_name": "main",
            }
    
    print(f"Table {table_name} created successfully:")
    print(f"  - Rows: {metadata['num_rows']}")
    
    return metadata

def _create_duckdb_table_from_sql_with_check(database_path: str, table_name: str, source_table: str, fallback_query: str) -> Dict[str, Any]:
    """
    Create DuckDB table from SQL query with table existence check.
    Converts DuckDBCreateTableOperator SQL functionality with table existence check to a regular function.
    """
    with duckdb.connect(database_path) as conn:
        print(f"Checking if source table {source_table} exists")
        
        # Check if source table exists
        table_exists_query = f"SELECT COUNT(*) FROM duckdb_tables() WHERE table_name = '{source_table}'"
        table_exists = conn.execute(table_exists_query).fetchone()[0] > 0
        
        if table_exists:
            print(f"Table {source_table} exists, creating table {table_name} from source")
            return _create_duckdb_table_from_sql(
                database_path=database_path,
                table_name=table_name,
                sql_query=f"CREATE OR REPLACE TABLE {table_name} AS (SELECT * FROM {source_table})"
            )
        else:
            print(f"Table {source_table} does not exist, creating table {table_name} with fallback data")
            return _create_duckdb_table_from_sql(
                database_path=database_path,
                table_name=table_name,
                sql_query=fallback_query
            )

def _create_duckdb_union_table(database_path: str, target_table: str, source_tables: List[str]) -> Dict[str, Any]:
    """
    Create union table from multiple existing tables.
    Converts DuckDBUnionTablesOperator functionality to a regular function.
    """
    with duckdb.connect(database_path) as conn:
        # Create union query
        union_clause = " UNION ALL ".join([f"SELECT * FROM {table}" for table in source_tables])
        query = f"CREATE OR REPLACE TABLE {target_table} AS ({union_clause})"
        
        print(f"Creating union table {target_table} from: {', '.join(source_tables)}")
        conn.execute(query)
        
        # Get metadata
        nrows = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
        
        # Get table information
        table_info_query = f"SELECT * FROM duckdb_tables() WHERE table_name = '{target_table}'"
        table_info = conn.execute(table_info_query).fetchone()
        
        if table_info:
            metadata = {
                "num_rows": nrows,
                "table_name": table_info[2],
                "database_name": table_info[0],
                "schema_name": table_info[1],
                "column_count": table_info[3] if len(table_info) > 3 else None,
                "estimated_size": table_info[4] if len(table_info) > 4 else None,
                "source_tables": source_tables
            }
        else:
            metadata = {
                "num_rows": nrows,
                "table_name": target_table,
                "database_name": "main",
                "schema_name": "main",
                "source_tables": source_tables
            }
    
    print(f"Union table {target_table} created successfully:")
    print(f"  - Rows: {metadata['num_rows']}")
    print(f"  - Source tables: {', '.join(source_tables)}")
    
    return metadata

def _sync_postgres_to_duckdb(postgres_config: Dict[str, Any], duckdb_database_path: str, tables_to_sync: List[str]) -> Dict[str, Any]:
    """
    Sync PostgreSQL tables to DuckDB using Sling.
    Converts SlingPostgresToDuckDBOperator functionality to a regular function.
    """
    # Create database directory if needed for DuckDB target
    db_path = Path(duckdb_database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Build source config
    source_config = {
        "type": "postgres",
        **postgres_config
    }
    
    # Build target config
    target_config = {
        "type": "duckdb",
        "instance": duckdb_database_path
    }
    
    # Build replication config
    streams = {}
    for table in tables_to_sync:
        streams[table] = None  # Use default configuration
    
    replication_config = {
        "source": "postgres",
        "target": "duckdb", 
        "defaults": {
            "mode": "full-refresh",
            "object": "{stream_schema}_{stream_table}"
        },
        "streams": streams
    }
    
    # Create temporary config file
    config_content = {
        "source": source_config,
        "target": target_config,
        **replication_config
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        import yaml
        yaml.dump(config_content, f)
        config_file_path = f.name
    
    try:
        print("Starting Sling replication...")
        print(f"Source: {source_config}")
        print(f"Target: {target_config}")
        print(f"Streams: {streams}")
        
        # Build Sling command
        cmd = ["sling", "run", config_file_path]
        
        print(f"Executing Sling command: {' '.join(cmd)}")
        
        # Execute the command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Parse output for metadata
        output_lines = result.stdout.split('\n')
        
        # Extract replication statistics
        replicated_tables = []
        total_rows = 0
        
        for line in output_lines:
            if 'rows' in line.lower() and 'replicated' in line.lower():
                # Try to extract table and row information
                parts = line.split()
                for i, part in enumerate(parts):
                    if part.isdigit():
                        total_rows += int(part)
                        break
            
            # Extract table names from streams
            for stream_name in streams.keys():
                if stream_name in line:
                    table_name = stream_name.replace('.', '_')
                    if table_name not in replicated_tables:
                        replicated_tables.append(table_name)
        
        metadata = {
            "replicated_tables": replicated_tables,
            "total_rows_estimated": total_rows,
            "source_type": source_config.get('type'),
            "target_type": target_config.get('type'),
            "streams": list(streams.keys()),
            "stdout": result.stdout,
            "stderr": result.stderr
        }
        
        print("Sling replication completed successfully")
        print(f"Replicated tables: {replicated_tables}")
        print(f"Output: {result.stdout}")
        
        if result.stderr:
            print(f"Stderr: {result.stderr}")
        
    except subprocess.CalledProcessError as e:
        print(f"Sling replication failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise
    
    finally:
        # Clean up temporary file
        Path(config_file_path).unlink(missing_ok=True)
    
    return metadata

def _run_dbt_command(dbt_project_dir: str, dbt_command: str = "run", models: Optional[List[str]] = None, target: Optional[str] = None) -> Dict[str, Any]:
    """
    Execute dbt commands.
    Converts DbtOperator functionality to a regular function.
    """
    # Build dbt command
    cmd = ["dbt", dbt_command]
    
    # Add project directory
    cmd.extend(["--project-dir", dbt_project_dir])
    
    # Add target if specified
    if target:
        cmd.extend(["--target", target])
    
    # Add models if specified
    if models:
        cmd.extend(["--models"] + models)
    
    # Add verbose flag for better logging
    cmd.append("--verbose")
    
    print(f"Executing dbt command: {' '.join(cmd)}")
    
    try:
        # Execute the command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=dbt_project_dir
        )
        
        # Parse dbt output for metadata
        output_lines = result.stdout.split('\n')
        
        # Extract run statistics
        models_run = []
        models_failed = []
        models_skipped = []
        
        for line in output_lines:
            if "RUN " in line or "PASS " in line:
                # Extract model name from dbt output
                parts = line.split()
                for i, part in enumerate(parts):
                    if part in ["RUN", "PASS"]:
                        if i + 1 < len(parts):
                            model_name = parts[i + 1]
                            if model_name not in models_run:
                                models_run.append(model_name)
                        break
            elif "FAIL " in line or "ERROR " in line:
                # Extract failed model name
                parts = line.split()
                for i, part in enumerate(parts):
                    if part in ["FAIL", "ERROR"]:
                        if i + 1 < len(parts):
                            model_name = parts[i + 1]
                            if model_name not in models_failed:
                                models_failed.append(model_name)
                        break
        
        metadata = {
            "command": dbt_command,
            "models_requested": models or [],
            "models_run": models_run,
            "models_failed": models_failed,
            "models_skipped": models_skipped,
            "success": len(models_failed) == 0,
            "stdout": result.stdout,
            "stderr": result.stderr
        }
        
        print(f"dbt {dbt_command} completed successfully")
        print(f"Models run: {models_run}")
        print(f"Models failed: {models_failed}")
        
        if result.stderr:
            print(f"Stderr: {result.stderr}")
        
        return metadata
        
    except subprocess.CalledProcessError as e:
        print(f"dbt {dbt_command} failed: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise

# Create DAG instance
complete_pipeline_dag = complete_birds_pipeline()

# ============================================================================
# EXAMPLE: Alternative @asset decorator approach
# ============================================================================
# 
# For simpler use cases, you can use the @asset decorator instead of defining
# separate Asset objects and using outlets. Here's an example:
#
# from airflow.sdk import asset
# 
# @asset(uri="duckdb:///data/db/example_table", schedule="@daily")
# def create_example_table():
#     """Create an example table - demonstrates @asset decorator usage."""
#     # This automatically creates:
#     # - An Asset with name='create_example_table' 
#     # - A DAG with dag_id='create_example_table'
#     # - A task with task_id='create_example_table' and outlet=Asset
#     pass
#
# # To consume this asset in another task/DAG:
# @task(inlets=[Asset("duckdb:///data/db/example_table")])
# def process_example_table():
#     """Process the example table data."""
#     pass
#
# For more complex pipelines like this one, using explicit Asset definitions
# and task decorators with inlets/outlets provides better control and clarity. 