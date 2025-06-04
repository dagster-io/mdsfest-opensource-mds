"""Basic Airflow asset scaffolding following the README plan."""

from airflow.sdk import asset

# Default retry policy required by tests
DEFAULT_ARGS = {"retries": 2}

@asset
def checklist_2020():
    """Placeholder asset for downloading 2016-2020 checklist data."""
    pass

@asset(deps=[checklist_2020])
def birds():
    """Placeholder asset combining checklist CSV files into one table."""
    pass

