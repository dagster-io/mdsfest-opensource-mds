# Quick Fix: Missing Airflow Variables

## Problem
You're getting this error:
```
AirflowRuntimeError: VARIABLE_NOT_FOUND: {'key': 'data_dir'}
```

## Root Cause
The asset-based pipeline references Airflow variables that haven't been set up yet:
- `data_dir` - Base directory for data storage
- `dbt_project_dir` - Path to dbt project

## Quick Solutions

### Option 1: Run the Setup Script (Recommended)
```bash
cd airflow_project
python setup_airflow_variables.py
```

### Option 2: Set Variables Manually via Airflow UI
1. Go to Airflow UI → **Admin** → **Variables**
2. Click **Create** and add these variables:

| Key | Value | Description |
|-----|-------|-------------|
| `data_dir` | `/path/to/your/airflow_project/data` | Base directory for data storage |
| `dbt_project_dir` | `/path/to/your/dbt_project` | Path to dbt project directory |

### Option 3: Set Variables via Airflow CLI
```bash
# Set data directory (adjust path as needed)
airflow variables set data_dir "/usr/local/airflow/dags/data"

# Set dbt project directory (adjust path as needed)  
airflow variables set dbt_project_dir "/usr/local/airflow/dbt_project"
```

### Option 4: Quick Docker/Container Fix
If you're running in Docker/containers, use these paths:
```bash
airflow variables set data_dir "/usr/local/airflow/data"
airflow variables set dbt_project_dir "/usr/local/airflow/dbt_project"
```

## Verify Variables are Set
Check in Airflow UI → Admin → Variables, or use CLI:
```bash
airflow variables list
```

## Create Directories
Make sure the directories exist:
```bash
mkdir -p /usr/local/airflow/data/{raw/checklist_data,db}
```

## After Setting Variables
1. **Refresh your DAG** in the Airflow UI
2. **Clear failed tasks** and retry them
3. **Trigger the raw data extraction DAG** to start the pipeline

## Expected Directory Structure
```
/usr/local/airflow/
├── data/
│   ├── raw/
│   │   └── checklist_data/
│   └── db/
│       └── osmds.db (created by pipeline)
└── dbt_project/
    ├── models/
    ├── dbt_project.yml
    └── profiles.yml
```

## Still Having Issues?
1. Check Airflow logs for more specific errors
2. Verify file permissions on directories
3. Ensure dbt project exists and is accessible
4. Check that all custom operators are properly installed 