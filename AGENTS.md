# Contributor Guide

## Dev Environment Tips

The Airflow and Dagster projects are in separate environments.
We use uv to manage our projects.

## Dagster

To work with the Dagster project:

### Initial setup
- cd dagster_project
- uv venv
- source .venv/bin/activate
- uv pip install -e '.[dev]'


### Run tests
- dg check defs
- dg check yaml

### List / inspect definitions 

- dg list defs --json
- dg list components --json
- dg utils inspect-component opensource_mds.components.CSVToDuckDB

### Additional information

Dagster Components are defined in `dagster_project/opensource_mds/components'
Assets are defined in 'dagster_project/opensource_mds/defs/assets.py'
The component instances are all defined in 'dagster_project/opensource_mds/defs/birds/defs.yaml'
New YAML instances can be appended to that file.

## Airflow

To work with Airflow, use the astro cli:

### Initial setup
- cd airflow_project

## Start the airflow environment
- astro dev start

## Get logs
- astro dev logs

### Test/lint
- astro dev parse
