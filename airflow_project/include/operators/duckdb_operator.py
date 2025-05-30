from typing import Any, Dict, Optional, List
from pathlib import Path

import duckdb
from airflow.models import BaseOperator
from airflow.utils.context import Context


class DuckDBCreateTableOperator(BaseOperator):
    """
    Creates DuckDB tables from CSV files.
    Replicates the functionality of Dagster DuckDB assets.
    """
    
    template_fields = ("database_path", "csv_file_path", "table_name", "sql_query")
    
    def __init__(
        self,
        database_path: str,
        table_name: str,
        csv_file_path: Optional[str] = None,
        sql_query: Optional[str] = None,
        create_database_dir: bool = True,
        outlets: Optional[List] = None,
        **kwargs
    ):
        """
        Initialize the operator.
        
        Args:
            database_path: Path to the DuckDB database file
            table_name: Name of the table to create
            csv_file_path: Path to CSV file to load (if creating from CSV)
            sql_query: Custom SQL query to execute (alternative to CSV loading)
            create_database_dir: Whether to create the database directory if it doesn't exist
            outlets: List of assets this operator produces (for Airflow 3 asset scheduling)
        """
        super().__init__(**kwargs)
        self.database_path = database_path
        self.table_name = table_name
        self.csv_file_path = csv_file_path
        self.sql_query = sql_query
        self.create_database_dir = create_database_dir
        # Set outlets for asset-based scheduling
        if outlets is not None:
            self.outlets = outlets
        
        if not csv_file_path and not sql_query:
            raise ValueError("Either csv_file_path or sql_query must be provided")
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute the table creation process.
        
        Returns:
            Dict containing metadata about the created table
        """
        # Create database directory if needed
        if self.create_database_dir:
            db_path = Path(self.database_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Connect to DuckDB
        with duckdb.connect(self.database_path) as conn:
            if self.csv_file_path:
                # Create table from CSV file
                self.log.info(f"Creating table {self.table_name} from CSV: {self.csv_file_path}")
                query = f"""
                CREATE OR REPLACE TABLE {self.table_name} AS (
                    SELECT * FROM read_csv_auto('{self.csv_file_path}', sample_size=-1)
                )
                """
            else:
                # Execute custom SQL query
                self.log.info(f"Creating table {self.table_name} with custom SQL")
                query = self.sql_query
            
            # Execute the query
            conn.execute(query)
            
            # Get table metadata
            nrows = conn.execute(f"SELECT COUNT(*) FROM {self.table_name}").fetchone()[0]
            
            # Get table information
            table_info_query = f"SELECT * FROM duckdb_tables() WHERE table_name = '{self.table_name}'"
            table_info = conn.execute(table_info_query).fetchone()
            
            if table_info:
                metadata = {
                    "num_rows": nrows,
                    "table_name": table_info[2],  # table_name
                    "database_name": table_info[0],  # database_name
                    "schema_name": table_info[1],  # schema_name
                    "column_count": table_info[3] if len(table_info) > 3 else None,
                    "estimated_size": table_info[4] if len(table_info) > 4 else None,
                }
            else:
                metadata = {
                    "num_rows": nrows,
                    "table_name": self.table_name,
                    "database_name": "main",
                    "schema_name": "main",
                }
        
        # Log metadata
        self.log.info(f"Table {self.table_name} created successfully:")
        self.log.info(f"  - Rows: {metadata['num_rows']}")
        self.log.info(f"  - Database: {metadata['database_name']}")
        self.log.info(f"  - Schema: {metadata['schema_name']}")
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(
            key=f"{self.table_name}_metadata",
            value=metadata
        )
        
        return metadata


class DuckDBUnionTablesOperator(BaseOperator):
    """
    Creates a union table from multiple existing tables.
    Specifically designed for combining birds_2020 and birds_2023 tables.
    """
    
    template_fields = ("database_path", "target_table", "source_tables")
    
    def __init__(
        self,
        database_path: str,
        target_table: str,
        source_tables: List[str],
        outlets: Optional[List] = None,
        **kwargs
    ):
        """
        Initialize the operator.
        
        Args:
            database_path: Path to the DuckDB database file
            target_table: Name of the target union table
            source_tables: List of source table names to union
            outlets: List of assets this operator produces (for Airflow 3 asset scheduling)
        """
        super().__init__(**kwargs)
        self.database_path = database_path
        self.target_table = target_table
        self.source_tables = source_tables
        # Set outlets for asset-based scheduling
        if outlets is not None:
            self.outlets = outlets
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute the union operation.
        
        Returns:
            Dict containing metadata about the created union table
        """
        with duckdb.connect(self.database_path) as conn:
            # Create union query
            union_clause = " UNION ALL ".join([f"SELECT * FROM {table}" for table in self.source_tables])
            query = f"CREATE OR REPLACE TABLE {self.target_table} AS ({union_clause})"
            
            self.log.info(f"Creating union table {self.target_table} from: {', '.join(self.source_tables)}")
            conn.execute(query)
            
            # Get metadata
            nrows = conn.execute(f"SELECT COUNT(*) FROM {self.target_table}").fetchone()[0]
            
            # Get table information
            table_info_query = f"SELECT * FROM duckdb_tables() WHERE table_name = '{self.target_table}'"
            table_info = conn.execute(table_info_query).fetchone()
            
            if table_info:
                metadata = {
                    "num_rows": nrows,
                    "table_name": table_info[2],
                    "database_name": table_info[0],
                    "schema_name": table_info[1],
                    "column_count": table_info[3] if len(table_info) > 3 else None,
                    "estimated_size": table_info[4] if len(table_info) > 4 else None,
                    "source_tables": self.source_tables
                }
            else:
                metadata = {
                    "num_rows": nrows,
                    "table_name": self.target_table,
                    "database_name": "main",
                    "schema_name": "main",
                    "source_tables": self.source_tables
                }
        
        self.log.info(f"Union table {self.target_table} created successfully:")
        self.log.info(f"  - Rows: {metadata['num_rows']}")
        self.log.info(f"  - Source tables: {', '.join(self.source_tables)}")
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(
            key=f"{self.target_table}_metadata",
            value=metadata
        )
        
        return metadata 