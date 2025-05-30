# Constants migrated from Dagster project
# Data source URLs
CHECKLIST_2020_URL = "https://clo-pfw-prod.s3.us-west-2.amazonaws.com/data/202306/PFW_all_2016_2020_June2023_Public.zip"
CHECKLIST_2023_URL = "https://clo-pfw-prod.s3.us-west-2.amazonaws.com/data/202306/PFW_all_2021_2023_June2023_Public.zip"
SITE_DESCRIPTION_DATA_URL = "https://clo-pfw-prod.s3.us-west-2.amazonaws.com/data/202306/PFW_count_site_data_public_June2023.zip"
SPECIES_TRANSLATION_DATA_URL = "https://clo-pfw-prod.s3.us-west-2.amazonaws.com/data/202306/PFW_spp_translation_table_May2023.zip"

# File paths (relative to data directory)
CL_2020_FPATH = "data/raw/checklist_data/PFW_all_2016_2020_June2023_Public.csv"
CL_2023_FPATH = "data/raw/checklist_data/PFW_all_2021_2023_June2023_Public.csv"
SITE_DATA_FPATH = "data/raw/checklist_data/PFW_count_site_data_public_June2023.csv"
SPECIES_TRANSLATION_FPATH = "data/raw/checklist_data/PFW_spp_translation_table_May2023.csv"

# Database configurations
DUCKDB_DATABASE = "data/db/osmds.db"

# Postgres connection for Sling
POSTGRES_CONFIG = {
    "host": "sample-data.popsql.io",
    "user": "demo", 
    "database": "marker",
    "password": "demo"
}

# Data directories
RAW_DATA_DIR = "data/raw/checklist_data"
DB_DIR = "data/db" 