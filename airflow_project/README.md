# Open Source MDS Birds Pipeline - Airflow Implementation

This project contains the **Airflow implementation** of the Open Source MDS Birds data pipeline, converted from the original Dagster implementation.

## 🚀 Quick Start

### Prerequisites
- **Apache Airflow** (with Astronomer Runtime)
- **DuckDB** for analytical storage
- **dbt** for data transformations  
- **Sling** for data replication
- **Docker** (for containerized deployment)

### 1. Setup Airflow Environment

```bash
# Clone and navigate to project
cd airflow_project

# Start Airflow (using Astronomer CLI)
astro dev start

# Set Airflow variables
astro dev run variables import include/config/airflow_variables.json
```

### 2. Verify Dependencies

The Dockerfile automatically installs:
- **Sling CLI** for PostgreSQL → DuckDB replication
- **dbt** with DuckDB adapter for transformations

### 3. Run the Pipeline

1. Open Airflow UI: `http://localhost:8080`
2. Find DAG: `opensource_mds_birds_pipeline`
3. Enable and trigger the DAG
4. Monitor task execution in the Graph View

## 📊 Pipeline Architecture

### Data Flow
```
┌─────────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Raw Data          │    │  Data            │    │  dbt            │
│   Extraction        │───▶│  Preparation     │───▶│  Transformations│
│   (Parallel)        │    │  (DuckDB Tables) │    │  (Analytics)    │
└─────────────────────┘    └──────────────────┘    └─────────────────┘
```

### Layer 1: Raw Data Extraction (Parallel)
- **Cornell Lab Data**: Downloads 2016-2020 and 2021-2023 bird observation ZIP files
- **Site & Species Data**: Downloads supporting lookup data
- **PostgreSQL Sync**: Replicates `tickets` and `events` tables via Sling

### Layer 2: Data Preparation
- **CSV to DuckDB**: Loads extracted CSV files into DuckDB tables
- **PostgreSQL Integration**: Creates tables from synced PostgreSQL data
- **Data Union**: Combines 2020 and 2023 bird observation data

### Layer 3: dbt Transformations
- **Staging Models**: Clean and prepare base data
- **Analytics Models**: Create analysis-ready datasets
- **Aggregations**: Generate summary tables (e.g., top birds by year)

## 🏗️ Project Structure

```
airflow_project/
├── dags/
│   ├── opensource_mds_pipeline.py      # Main DAG definition
│   └── task_groups/
│       └── raw_data_group.py           # Raw data extraction tasks
├── include/
│   ├── operators/                      # Custom operators
│   │   ├── download_extract_operator.py
│   │   ├── duckdb_operator.py
│   │   ├── sling_operator.py
│   │   └── dbt_operator.py
│   ├── utils/
│   │   └── constants.py                # URLs and configuration
│   └── config/
│       └── airflow_variables.json      # Airflow variables
├── Dockerfile                          # Container setup with Sling & dbt
├── requirements.txt                    # Python dependencies
├── MIGRATION_GUIDE.md                  # Conversion documentation
└── README.md                          # This file
```

## 🔧 Custom Operators

### `DownloadAndExtractOperator`
- **Purpose**: Download ZIP files from URLs and extract contents
- **Replaces**: Dagster `BirdChecklist` component
- **Features**: Progress logging, metadata tracking, automatic directory creation

### `DuckDBCreateTableOperator` 
- **Purpose**: Create DuckDB tables from CSV files or SQL queries
- **Replaces**: Dagster DuckDB assets
- **Features**: Table metadata collection, error handling, performance logging

### `SlingPostgresToDuckDBOperator`
- **Purpose**: Replicate PostgreSQL tables to DuckDB
- **Replaces**: Dagster Sling assets  
- **Features**: Configurable sync modes, progress tracking

### `DbtOperator` Family
- **Purpose**: Execute dbt commands (run, build, test)
- **Replaces**: Dagster dbt assets
- **Features**: Model-specific execution, detailed logging, failure handling

## 📈 Monitoring & Observability

### Task Metadata
All operators push detailed metadata to **XCom**:
- Download statistics (file counts, sizes, timing)
- Table creation metrics (row counts, schema info)
- dbt execution results (models run, failures, timing)

### Logging
Comprehensive logging at each layer:
- **Raw Data**: Download progress, extraction details
- **Preparation**: Table creation stats, data quality metrics  
- **Transformations**: dbt model execution, test results

### Airflow UI Features
- **Graph View**: Visual pipeline dependencies
- **Gantt Chart**: Execution timing analysis
- **Task Logs**: Detailed operator logs
- **XCom Browser**: Metadata inspection

## 🔄 Migration from Dagster

### Key Differences

| Aspect | Dagster | Airflow |
|--------|---------|---------|
| **Execution Model** | Asset-based | Task-based |
| **Dependencies** | Automatic from inputs/outputs | Explicit via `>>` operator |
| **Metadata** | Built-in asset metadata | XCom + logging |
| **Configuration** | Resources + environment | Variables + connections |
| **Scheduling** | Asset-driven | DAG-based |

### Component Mapping

| Dagster | Airflow | Notes |
|---------|---------|-------|
| `BirdChecklist` component | `DownloadAndExtractOperator` | Direct functional equivalent |
| `@asset` decorators | Custom operators | Encapsulates asset logic |
| Asset dependencies | Task dependencies (`>>`) | Explicit dependency declaration |
| Resource configurations | Operator parameters | Configuration via constructor |

### Benefits of Airflow Version
- **Explicit Control**: Clear task dependencies and execution order
- **Operational Maturity**: Rich UI, monitoring, and alerting ecosystem
- **Task Isolation**: Independent task execution with retry logic
- **Scalability**: Distributed execution with multiple workers

## 🚨 Troubleshooting

### Common Issues

1. **File Path Errors**
   ```bash
   # Check Airflow variables
   astro dev run variables list
   
   # Verify directory structure
   docker exec <container> ls -la /opt/airflow/data
   ```

2. **Sling Not Found**
   ```bash
   # Verify Sling installation
   docker exec <container> sling --version
   
   # Check PATH
   docker exec <container> which sling
   ```

3. **dbt Errors**
   ```bash
   # Test dbt configuration
   docker exec <container> dbt debug --project-dir /opt/airflow/dbt_project
   ```

4. **PostgreSQL Connection**
   ```bash
   # Test connectivity
   docker exec <container> curl -v sample-data.popsql.io:5432
   ```

### Performance Optimization

- **Parallel Execution**: Raw data tasks run concurrently
- **Resource Allocation**: Adjust worker resources for large datasets
- **dbt Threading**: Use `--threads` for parallel model execution
- **Retry Strategy**: Built-in exponential backoff for transient failures

## 📚 Additional Resources

- **[Migration Guide](MIGRATION_GUIDE.md)**: Detailed conversion documentation
- **[PLAN.md](PLAN.md)**: Original conversion planning document
- **[Astronomer Docs](https://docs.astronomer.io/)**: Airflow deployment guide
- **[dbt Documentation](https://docs.getdbt.com/)**: dbt transformation patterns
- **[Sling Documentation](https://docs.slingdata.io/)**: Data replication setup

## 🤝 Contributing

1. **Task Enhancement**: Improve operator functionality
2. **Monitoring**: Add custom metrics and alerting
3. **Testing**: Implement data quality checks
4. **Documentation**: Enhance operator docstrings
5. **Performance**: Optimize for larger datasets

## 📄 License

This project maintains the same license as the original Dagster implementation.

---

**Migration Status**: ✅ **Complete**  
**Original Dagster Pipeline**: Fully replicated in Airflow  
**Production Ready**: Yes, with proper environment setup
