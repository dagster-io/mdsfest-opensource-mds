"""Basic Airflow asset DAG scaffold following the README plan."""

from datetime import datetime

try:
    from airflow.decorators import asset, asset_group
except Exception:  # pragma: no cover - Airflow not available during static checks
    def asset(_func=None, **_kwargs):
        def decorator(func):
            return func
        return decorator

    def asset_group(*_args, **_kwargs):  # type: ignore
        def decorator(funcs):
            return None
        return decorator

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

# Group the assets into a DAG
assets_dag = asset_group(
    dag_id="assets_pipeline",
    assets=[checklist_2020, birds],
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["mds", "assets"],
)
