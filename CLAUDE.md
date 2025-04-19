# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Open Source MDS is a Modern Data Stack demonstration project that processes bird observation data from Cornell Lab's Project FeederWatch. It showcases a complete data pipeline using Dagster orchestration, dbt transformations, DuckDB storage, and Evidence.dev visualization.

There is an Airflow project and a Dagster project.

The Dagster project is in ./dagster_project
The Airflow project is in ./airflow_project

### Common Commands for the Airflow Project

The Airflow project has not yet been completed!

## Common Commands for the Dagster Project

### Setup and Installation
```bash
make install          # Create venv, install dependencies, setup Evidence
```

### Running the Data Pipeline
```bash
make run-dagster      # Start Dagster webserver at localhost:3000
dagster dev           # Alternative way to start Dagster
```

### Data Visualization
```bash
make run-evidence     # Start Evidence dev server at localhost:4000
```

### Data Management
```bash
make clean           # Remove all data files from data/ directory
```

### Testing
- Tests are located in `opensource_mds_tests/`
- Run tests with standard pytest commands

## Architecture

### Core Components
- **Dagster**: Primary orchestration platform with asset-based pipeline management
- **DuckDB**: In-process analytical database (stored at `/data/db/osmds.db`)
- **dbt**: Data transformation layer (project name: "dbirdt")
- **Sling**: Data replication from Postgres sources
- **Evidence.dev**: BI visualization frontend
- **Airflow**: Alternative orchestration option in `opensource_mds_airflow/`

### Asset Pipeline Flow
1. **Raw ingestion**: Download bird checklists, site data, species translations
2. **Data preparation**: Transform to `birds`, `species`, `sites`, `tickets`, `events` tables
3. **dbt transformations**: Create analytical models (`all_birds`, `daily_tickets`, `top_birds_by_year`)
4. **Visualization**: Evidence.dev reports consume dbt models

### Key File Locations
- **Dagster definitions**: `opensource_mds/definitions.py` (entry point)
- **Asset definitions**: `opensource_mds/defs/assets.py`
- **Resources**: `opensource_mds/defs/resources.py` (DuckDB, Sling, dbt configs)
- **dbt models**: `dbt_project/models/`
- **Evidence reports**: `dbt_project/reports/`

### Development Workflow
1. Start Dagster with `make run-dagster`
2. Access UI at http://localhost:3000/asset-groups/
3. Click "Materialize all" to run end-to-end pipeline
4. View results with Evidence at http://localhost:4000

### Data Sources
- Cornell Lab Project FeederWatch (2016-2023 bird observations)
- Demo Postgres database (sample tickets/events)
- Optional Mastodon API integration via Steampipe

## Branch Context
- Currently on `componetify` branch
- Main branch available for PRs
- Project includes both Dagster and Airflow orchestration options
