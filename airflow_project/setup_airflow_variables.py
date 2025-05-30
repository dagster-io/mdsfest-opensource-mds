#!/usr/bin/env python3
"""
Setup script for Airflow variables required by the opensource MDS birds pipeline.
Run this script to configure the necessary Airflow variables.

Usage:
    python setup_airflow_variables.py
    
Or set variables manually in Airflow UI:
    Admin -> Variables -> Create
"""

import os
import subprocess
import sys
from pathlib import Path


def set_airflow_variable(key: str, value: str, description: str = ""):
    """Set an Airflow variable using the CLI."""
    try:
        cmd = ["airflow", "variables", "set", key, value]
        if description:
            cmd.extend(["--description", description])
        
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"✅ Set variable '{key}' = '{value}'")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to set variable '{key}': {e}")
        print(f"   Error output: {e.stderr}")
        return False
    except FileNotFoundError:
        print("❌ Airflow CLI not found. Please install Airflow or set variables manually.")
        return False


def get_project_paths():
    """Get the project directory paths."""
    # Get the current script directory
    script_dir = Path(__file__).parent.absolute()
    
    # Data directory (where DuckDB and raw data will be stored)
    data_dir = script_dir / "data"
    
    # dbt project directory
    dbt_project_dir = script_dir.parent / "dbt_project"
    
    return {
        "data_dir": str(data_dir),
        "dbt_project_dir": str(dbt_project_dir)
    }


def create_directories(paths):
    """Create necessary directories."""
    data_dir = Path(paths["data_dir"])
    
    # Create data directory structure
    directories_to_create = [
        data_dir,
        data_dir / "raw" / "checklist_data",
        data_dir / "db"
    ]
    
    for directory in directories_to_create:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"📁 Created directory: {directory}")


def main():
    print("🚀 Setting up Airflow variables for opensource MDS birds pipeline...")
    print("=" * 60)
    
    # Get project paths
    paths = get_project_paths()
    
    # Create necessary directories
    print("\n📁 Creating directories...")
    create_directories(paths)
    
    # Define variables to set
    variables = {
        "data_dir": {
            "value": paths["data_dir"],
            "description": "Base directory for storing data files, DuckDB database, and raw data"
        },
        "dbt_project_dir": {
            "value": paths["dbt_project_dir"], 
            "description": "Path to the dbt project directory"
        }
    }
    
    print("\n🔧 Setting Airflow variables...")
    success_count = 0
    
    for key, config in variables.items():
        if set_airflow_variable(key, config["value"], config["description"]):
            success_count += 1
    
    print("\n" + "=" * 60)
    print(f"✅ Successfully set {success_count}/{len(variables)} variables")
    
    if success_count < len(variables):
        print("\n⚠️  Some variables failed to set. You can set them manually:")
        print("   1. Go to Airflow UI -> Admin -> Variables")
        print("   2. Click 'Create' and add the following:")
        
        for key, config in variables.items():
            print(f"      - Key: {key}")
            print(f"        Value: {config['value']}")
            print(f"        Description: {config['description']}")
            print()
    
    # Verify dbt project exists
    dbt_project_path = Path(paths["dbt_project_dir"])
    if not dbt_project_path.exists():
        print(f"\n⚠️  dbt project directory not found: {dbt_project_path}")
        print("   Make sure your dbt project is in the correct location.")
    else:
        print(f"\n✅ dbt project found: {dbt_project_path}")
    
    print("\n🎉 Setup complete! You can now run the asset-based pipeline.")
    print("\nNext steps:")
    print("1. Verify variables in Airflow UI: Admin -> Variables")
    print("2. Enable the DAGs in Airflow UI")
    print("3. Trigger the raw data extraction DAG to start the pipeline")


if __name__ == "__main__":
    main() 