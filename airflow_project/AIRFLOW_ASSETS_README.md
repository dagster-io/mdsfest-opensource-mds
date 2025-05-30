# Airflow 3 Asset-Based Pipeline Architecture

This document explains the conversion of the open source MDS birds pipeline to use Airflow 3's asset-based scheduling.

## Overview

The pipeline has been restructured from a single monolithic DAG to a collection of asset-aware DAGs that are triggered based on data updates rather than time-based scheduling. This creates a more reactive and efficient data pipeline.

## Asset-Based Architecture

### Key Concepts

- **Assets**: Represent data entities (files, tables, models) with unique names
- **Asset Events**: Created when producer tasks successfully update assets
- **Producer Tasks**: Tasks that create or update assets (specified via `outlets` parameter)
- **Consumer DAGs**: DAGs scheduled to run when their asset dependencies are updated
- **Asset Schedules**: Trigger DAGs based on asset updates instead of cron expressions

### Benefits

1. **Data-Aware Scheduling**: DAGs only run when their input data has actually changed
2. **Clear Dependencies**: Explicit lineage between data assets across DAGs
3. **Improved Monitoring**: Track data flow through the pipeline in the Airflow UI
4. **Resource Efficiency**: Eliminate unnecessary runs when upstream data hasn't changed
5. **Better Failure Handling**: Failed upstream tasks don't trigger downstream processing

## Pipeline Structure

The pipeline now consists of four separate DAGs:

### 1. Raw Data Extraction DAG (`opensource_mds_birds_pipeline_raw_data`)

**Schedule**: Time-based (`@daily`)
**Purpose**: Downloads and syncs raw data from external sources

**Assets Produced**:
- `raw_checklist_2020_data` - Cornell Lab 2016-2020 checklist data
- `raw_checklist_2023_data` - Cornell Lab 2021-2023 checklist data  
- `raw_site_data` - Site description data
- `raw_species_translation_data` - Species translation table
- `raw_postgres_tickets_data` - PostgreSQL tickets table sync
- `raw_postgres_events_data` - PostgreSQL events table sync

### 2. Data Preparation DAG (`opensource_mds_birds_pipeline_data_prep`)

**Schedule**: Asset-based (triggered by `RAW_DATA_ASSETS`)
**Purpose**: Creates prepared DuckDB tables from raw data

**Assets Consumed**: All raw data assets
**Assets Produced**:
- `duckdb_birds_2020_table` - DuckDB table from 2020 checklist data
- `duckdb_birds_2023_table` - DuckDB table from 2023 checklist data
- `duckdb_species_table` - DuckDB species translation table
- `duckdb_sites_table` - DuckDB sites table
- `duckdb_tickets_table` - DuckDB tickets table
- `duckdb_events_table` - DuckDB events table
- `duckdb_birds_union_table` - Combined birds table (2020 + 2023)

### 3. dbt Transformations DAG (`opensource_mds_birds_pipeline_dbt`)

**Schedule**: Asset-based (triggered by `PREPARED_TABLE_ASSETS`)
**Purpose**: Runs dbt models for analytics-ready data

**Assets Consumed**: All prepared DuckDB tables
**Assets Produced**:
- `dbt_all_birds_model` - dbt all_birds model
- `dbt_daily_tickets_model` - dbt daily_tickets model
- `dbt_staging_models` - dbt staging models (stg_*)
- `dbt_top_birds_by_year_model` - Final analytics model

### 4. Summary DAG (`opensource_mds_birds_pipeline_summary`)

**Schedule**: Asset-based (triggered by `dbt_top_birds_by_year_model`)
**Purpose**: Final pipeline summary and notifications

**Assets Consumed**: Final dbt model
**Assets Produced**: None (terminal DAG)

## Asset Flow Diagram

```
Raw Data Sources
       ↓
┌─────────────────────────────────────────────────────────────┐
│  Raw Data Extraction DAG (@daily)                          │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │ Download 2020   │  │ Download 2023   │  │ Sync Postgres│ │
│  │ Checklist       │  │ Checklist       │  │ Data         │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
       ↓ (produces raw data assets)
┌─────────────────────────────────────────────────────────────┐
│  Data Preparation DAG (asset-triggered)                    │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │ Create DuckDB   │  │ Create DuckDB   │  │ Union Birds │ │
│  │ Tables          │  │ Lookup Tables   │  │ Tables      │ │
│  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────┘
       ↓ (produces prepared table assets)
┌─────────────────────────────────────────────────────────────┐
│  dbt Transformations DAG (asset-triggered)                 │
│  ┌─────────────────┐  ┌─────────────────┐                  │
│  │ dbt Build       │  │ dbt Run Final   │                  │
│  │ (staging +      │→ │ (top_birds_by   │                  │
│  │  main models)   │  │  _year)         │                  │
│  └─────────────────┘  └─────────────────┘                  │
└─────────────────────────────────────────────────────────────┘
       ↓ (produces dbt model assets)
┌─────────────────────────────────────────────────────────────┐
│  Summary DAG (asset-triggered)                             │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │ Final Summary & Notifications                           │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Operator Updates

All custom operators have been updated to support the `outlets` parameter:

- `DownloadAndExtractOperator`
- `DuckDBCreateTableOperator`
- `DuckDBUnionTablesOperator`
- `DbtOperator` (and its subclasses)
- `SlingReplicationOperator` and `SlingPostgresToDuckDBOperator`

Example usage:
```python
create_table_task = DuckDBCreateTableOperator(
    task_id="create_table",
    # ... other parameters ...
    outlets=[Asset("my_table_asset")],  # Asset produced by this task
)
```

## Asset Naming Convention

Assets follow a consistent naming pattern:

- **Raw Data**: `raw_{source}_{type}_data` (e.g., `raw_checklist_2020_data`)
- **DuckDB Tables**: `duckdb_{table_name}_table` (e.g., `duckdb_birds_2020_table`)
- **dbt Models**: `dbt_{model_name}_model` (e.g., `dbt_all_birds_model`)

## Deployment and Configuration

### Prerequisites

- Airflow 3.x
- All custom operators support `outlets` parameter
- Asset dependencies configured correctly

### DAG Deployment

Deploy all four DAGs to your Airflow environment:

1. `opensource_mds_birds_pipeline_raw_data.py`
2. `opensource_mds_birds_pipeline_data_prep.py` 
3. `opensource_mds_birds_pipeline_dbt.py`
4. `opensource_mds_birds_pipeline_summary.py`

### Monitoring

Use the Airflow UI **Assets** tab to:

- View asset dependency graph
- Monitor asset event history
- Track data lineage across DAGs
- Debug asset scheduling issues

## Migration from Original Pipeline

### Key Changes

1. **Single DAG → Multiple DAGs**: Separated concerns into focused DAGs
2. **Time Schedule → Asset Schedule**: Most DAGs now trigger on data updates
3. **Implicit Dependencies → Explicit Assets**: Clear data lineage through named assets
4. **Operator Updates**: All operators support `outlets` parameter

### Migration Benefits

- **Reduced Resource Usage**: Tasks only run when needed
- **Better Observability**: Clear view of data dependencies
- **Improved Reliability**: Failed tasks don't cascade unnecessarily
- **Easier Maintenance**: Focused DAGs with clear responsibilities

## Troubleshooting

### Common Issues

1. **Asset Not Triggering Downstream DAG**
   - Check that producer task completed successfully
   - Verify asset names match exactly between producer and consumer
   - Ensure `outlets` parameter is set correctly

2. **DAG Not Scheduling**
   - Verify asset dependencies in DAG definition
   - Check that upstream assets have been created at least once
   - Review scheduler logs for asset-related errors

3. **Missing Asset Events**
   - Confirm producer tasks are completing successfully
   - Check that `outlets` parameter includes correct assets
   - Verify operator inheritance supports outlets

### Debugging Commands

```bash
# View asset information
airflow assets list

# Check asset events
airflow assets events

# View DAG asset dependencies  
airflow dags show <dag_id> --assets
```

## Future Enhancements

Potential improvements using Airflow 3 asset features:

1. **Conditional Asset Scheduling**: Use `&` and `|` operators for complex dependencies
2. **Asset Aliases**: Dynamic asset names for parameterized workflows
3. **Asset Metadata**: Attach rich metadata to asset events
4. **Event-Driven Scheduling**: Integrate with message queues for real-time processing

## References

- [Airflow 3 Assets Documentation](https://www.astronomer.io/docs/learn/airflow-datasets/)
- [Asset-Based Scheduling Guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html) 