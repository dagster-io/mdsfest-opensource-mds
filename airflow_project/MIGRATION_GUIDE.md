# Dagster to Airflow Migration Guide

This document explains the conversion of the Open Source MDS Birds pipeline from Dagster to Airflow.

## Architecture Overview

### Original Dagster Pipeline
- **Components**: Used `BirdChecklist` components for dynamic ZIP download assets
- **Assets**: Functional approach with asset dependencies
- **Resources**: DuckDB, Sling, and dbt resources
- **Execution**: Asset-based execution with automatic dependency resolution

### Converted Airflow Pipeline  
- **Operators**: Custom operators replacing Dagster components and assets
- **Task Groups**: Logical grouping of related tasks
- **Dependencies**: Explicit task dependencies using `>>` operator
- **Execution**: Task-based execution with DAG scheduling

## Component Mapping

| Dagster Component/Asset | Airflow Equivalent | Purpose |
|------------------------|-------------------|---------|
| `BirdChecklist` component | `DownloadAndExtractOperator` | Download and extract ZIP files |
| `site_description_data` asset | `DownloadAndExtractOperator` | Download site description data |
| `species_translation_data` asset | `DownloadAndExtractOperator` | Download species translation data |
| `sling_sync_assets` | `SlingPostgresToDuckDBOperator` | PostgreSQL to DuckDB sync |
| `birds`, `species`, `sites` assets | `DuckDBCreateTableOperator` | Create DuckDB tables |
| `dbt_birds` asset | `DbtBuildOperator`, `DbtRunOperator` | Run dbt transformations |

## Setup Instructions

### 1. Airflow Variables Configuration

Set the following Airflow variables (via UI or CLI):

```bash
# Using Airflow CLI
airflow variables set data_dir "/opt/airflow/data"
airflow variables set dbt_project_dir "/opt/airflow/dbt_project"

# Or import from JSON file
airflow variables import include/config/airflow_variables.json
```

### 2. Directory Structure Setup

Ensure the following directories exist and are accessible to Airflow:

```
/opt/airflow/
├── data/
│   ├── raw/
│   │   └── checklist_data/     # ZIP extraction destination
│   └── db/                     # DuckDB database location
├── dbt_project/                # dbt project directory
└── dags/                       # Airflow DAGs
```

### 3. External Dependencies

#### Sling Installation
Sling must be available in the PATH or specify the full path in the operator:

```bash
# Download and install Sling
curl -LO "https://github.com/slingdata-io/sling-cli/releases/latest/download/sling_linux_amd64.tar.gz"
tar -xzf sling_linux_amd64.tar.gz
sudo mv sling /usr/local/bin/
```

#### dbt Setup
Ensure dbt is installed and configured:

```bash
# Install dbt (if not already available)
pip install dbt-core dbt-duckdb

# Test dbt configuration
cd /opt/airflow/dbt_project
dbt debug
```

### 4. PostgreSQL Connection
The pipeline connects to the sample PostgreSQL database. No additional setup required, but verify connectivity:

- Host: `sample-data.popsql.io`
- User: `demo`
- Database: `marker`
- Password: `demo`

## Pipeline Structure

### Layer 1: Raw Data Extraction (`raw_data_extraction` TaskGroup)
- **Parallel Execution**: All tasks run simultaneously
- **Tasks**:
  - `download_checklist_2020`: Downloads 2016-2020 bird data
  - `download_checklist_2023`: Downloads 2021-2023 bird data  
  - `download_site_description_data`: Downloads site description data
  - `download_species_translation_data`: Downloads species translation data
  - `sync_postgres_to_duckdb`: Syncs PostgreSQL tables to DuckDB

### Layer 2: Data Preparation (`data_preparation` TaskGroup)
- **Dependencies**: Waits for raw data extraction to complete
- **Tasks**:
  - `create_birds_2020_table`: Creates DuckDB table from 2020 CSV
  - `create_birds_2023_table`: Creates DuckDB table from 2023 CSV
  - `create_species_table`: Creates species lookup table
  - `create_sites_table`: Creates sites lookup table
  - `create_tickets_table`: Creates tickets table from PostgreSQL sync
  - `create_events_table`: Creates events table from PostgreSQL sync
  - `create_birds_union_table`: Combines 2020 and 2023 bird data

### Layer 3: dbt Transformations (`dbt_transformations` TaskGroup)
- **Dependencies**: Waits for data preparation to complete
- **Tasks**:
  - `dbt_run_all_birds`: Builds primary dbt models
  - `dbt_run_top_birds_by_year`: Runs final aggregation model

## Key Differences from Dagster

### 1. Explicit Dependencies
- **Dagster**: Automatic dependency resolution based on asset inputs/outputs
- **Airflow**: Explicit dependencies using `>>` operator

### 2. Metadata Handling
- **Dagster**: Built-in asset metadata system
- **Airflow**: XCom for metadata passing between tasks

### 3. Configuration
- **Dagster**: Resources and environment variables
- **Airflow**: Variables and connections system

### 4. Execution Model
- **Dagster**: Asset-centric execution
- **Airflow**: Task-centric execution with DAG scheduling

## Monitoring and Debugging

### Task Logs
Check individual task logs in the Airflow UI for:
- Download progress and file extraction details
- DuckDB table creation statistics
- dbt model run results
- Sling replication status

### XCom Values
Metadata is stored in XComs with keys like:
- `{asset_name}_metadata`: Download and extraction metadata
- `{table_name}_metadata`: DuckDB table creation metadata
- `dbt_{command}_metadata`: dbt execution metadata
- `sling_replication_metadata`: Sling sync metadata

### Common Issues

1. **File Path Issues**: Ensure Airflow variables point to correct directories
2. **Permission Issues**: Verify Airflow has write access to data directories
3. **Missing Dependencies**: Check that Sling and dbt are available in PATH
4. **Network Issues**: Verify connectivity to Cornell Lab URLs and PostgreSQL

## Performance Considerations

- **Parallel Execution**: Raw data tasks run in parallel for faster completion
- **Resource Usage**: Large ZIP files may require adequate disk space
- **dbt Models**: Consider using `--threads` for parallel dbt execution
- **Retry Logic**: Built-in retry mechanism for transient failures

## Future Enhancements

1. **Incremental Loading**: Implement incremental data loading strategies
2. **Data Quality**: Add data quality checks between layers
3. **Alerting**: Implement custom alerting for pipeline failures
4. **Scheduling**: Fine-tune scheduling based on data availability
5. **Resource Optimization**: Optimize resource allocation for large datasets 