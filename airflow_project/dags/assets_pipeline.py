"""Basic Airflow asset scaffolding following the README plan."""

from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.sdk import Asset

# Default retry policy required by tests
DEFAULT_ARGS = {"retries": 2}

# Define assets
checklist_2020_asset = Asset("checklist_2020")
birds_asset = Asset("birds")

@dag(
    dag_id="assets_pipeline",
    default_args=DEFAULT_ARGS,
    description="Basic asset pipeline for bird observation data",
    schedule=[checklist_2020_asset],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["assets", "pipeline"],
)
def assets_pipeline():
    
    @task(outlets=[checklist_2020_asset])
    def checklist_2020():
        """Placeholder asset for downloading 2016-2020 checklist data."""
        print("Processing checklist 2020 data")
        return "checklist_2020_complete"

    @task(outlets=[birds_asset])
    def birds():
        """Placeholder asset combining checklist CSV files into one table."""
        print("Processing birds data")
        return "birds_complete"
    
    checklist_2020() >> birds()

# Instantiate the DAG
dag_instance = assets_pipeline()

