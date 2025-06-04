# Airflow 3 Asset Conversion Plan

This document sketches a plan for how to build an Airflow 3 project that mirrors the Dagster asset pipeline found in `dagster_project`.  It uses the new Airflow "assets" API in order to track lineage and schedule DAGs based on data dependencies.

## 1. Overview

The Dagster project defines a series of assets that ingest raw zip files, load CSV files into DuckDB, replicate data from Postgres via Sling, and run dbt models.  These assets are described in `dg list defs` output and the YAML component definitions under `opensouce_mds/defs/birds/defs.yaml`.

Airflow 3 supports a similar concept via the `@asset` decorator and `Dataset` objects.  Assets can be grouped into asset DAGs and scheduled based on upstream dependencies.  Below is a high‑level plan to reproduce the Dagster assets in Airflow.

## 2. Required Resources

- **DuckDB** – use the `DuckDBHook` (or run `duckdb` CLI commands via Python tasks) to create and query tables.
- **Sling** – invoke the `sling` CLI just as in the Dockerfile.  Capture the replication logs so they can be pushed as XComs or logged.
- **dbt** – execute `dbt build` using `BashOperator` or the `DbtBuildOperator` from the `astronomer-providers` package.

These resources should be installed in `requirements.txt` and imported in the DAG files.

## 3. Asset Mapping

| Airflow Asset Name  | Upstream Assets / Datasets | Notes |
|---------------------|---------------------------|------|
| `checklist_2020`    | *(none)*                  | downloads 2016‑2020 zip and extracts to CSV |
| `checklist_2023`    | *(none)*                  | downloads 2021‑2023 zip and extracts to CSV |
| `site_description_data` | *(none)*              | downloads site metadata zip |
| `species_translation_data` | *(none)*           | downloads species translation zip |
| `species`           | `species_translation_data` | load CSV into DuckDB |
| `sites`             | `site_description_data`    | load CSV into DuckDB |
| `birds`             | `checklist_2020`, `checklist_2023` | combine both CSV files into one table |
| `tickets`           | `sling_sync_assets`        | copy from Sling‑replicated table |
| `events`            | `sling_sync_assets`        | copy from Sling‑replicated table |
| `public/events`     | *(external)*               | raw table produced by Sling |
| `public/tickets`    | *(external)*               | raw table produced by Sling |
| `target/public/events` | `public/events`         | final table replicated to target |
| `target/public/tickets` | `public/tickets`       | final table replicated to target |
| `all_birds`         | `birds`, `sites`, `species` | dbt model |
| `daily_tickets`     | `tickets`                  | dbt model |
| `top_birds_by_year` | `all_birds`                | dbt model |

The naming of the Airflow assets should match the Dagster asset keys exactly.  Use the same grouping (`raw_data`, `prepared`, etc.) via Airflow asset groups.

## 4. Implementation Steps

1. **Create Airflow DAG package**
   - Add a Python module under `airflow_project/dags/assets_pipeline.py`.
   - Use the `asset` decorator from `airflow.decorators` to define each asset as a Python function.
   - Define `dataset = Dataset("checklist_2020")` style objects for each asset so that dependencies can be declared.

2. **Download Assets**
   - For `checklist_2020`, `checklist_2023`, `site_description_data`, and `species_translation_data` create asset functions that download the zip file, extract to `data/raw/checklist_data`, and push metadata via XCom.

3. **Load CSVs into DuckDB**
   - Define asset functions `species` and `sites` that depend on the dataset from their respective downloads.  Use `DuckDBHook` or `duckdb` Python library to create `main.species` and `main.sites` tables.
   - Asset `birds` reads both CSV files and unions them into `main.birds` similar to the Dagster `DataCombiner` component.

4. **Sling Replication**
   - Asset `sling_sync_assets` executes `sling replicate` to copy the Postgres tables to DuckDB.  The `public/events` and `public/tickets` assets can reference the tables created by Sling.
   - Downstream assets `events`, `tickets`, `target/public/events`, and `target/public/tickets` copy or move the Sling tables as needed.

5. **dbt Models**
   - Use the `DbtBuildOperator` within asset functions for `all_birds`, `daily_tickets`, and `top_birds_by_year`.
   - Each dbt asset depends on its upstream raw tables (`birds`, `sites`, etc.) by referencing the dataset objects.

6. **Scheduling & Groups**
   - Group assets in Airflow using the `@asset_group` decorator (or `AssetDAG`) to match the Dagster groups:
     - `raw_data`: download assets
     - `prepared`: DuckDB tables and dbt models
     - `default`: sling replication
   - Schedule the group to run daily or on demand, with dataset‑triggered runs for dbt assets.

7. **Testing**
   - Extend the example `test_dag_example.py` to ensure all asset DAGs load correctly and that each asset has tags and retries configured.

8. **Docker Image**
   - Update `Dockerfile` to install Airflow 3 runtime base image (`astro-runtime:3.0`) and include Sling, DuckDB, and dbt installations similar to the Dagster image.

## 5. Next Steps

- Start with a minimal DAG containing one or two assets to verify connectivity to DuckDB and Sling.
- Gradually port each Dagster component to its Airflow asset equivalent.
- Once all assets are implemented, use Airflow's lineage UI to visualize dependencies.
- Compare results of running the Dagster pipeline versus the Airflow pipeline to ensure parity.

