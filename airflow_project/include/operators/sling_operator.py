import subprocess
import tempfile
from typing import Any, Dict, List, Optional
from pathlib import Path

from airflow.models import BaseOperator
from airflow.utils.context import Context


class SlingReplicationOperator(BaseOperator):
    """
    Replicates data from PostgreSQL to DuckDB using Sling.
    Mirrors the functionality of Dagster Sling assets.
    """
    
    template_fields = ("source_config", "target_config", "replication_config")
    
    def __init__(
        self,
        source_config: Dict[str, Any],
        target_config: Dict[str, Any],
        replication_config: Dict[str, Any],
        sling_binary_path: str = "sling",
        outlets: Optional[List] = None,
        **kwargs
    ):
        """
        Initialize the operator.
        
        Args:
            source_config: Source database configuration
            target_config: Target database configuration  
            replication_config: Sling replication configuration
            sling_binary_path: Path to the Sling binary
            outlets: List of assets this operator produces (for Airflow 3 asset scheduling)
        """
        super().__init__(**kwargs)
        self.source_config = source_config
        self.target_config = target_config
        self.replication_config = replication_config
        self.sling_binary_path = sling_binary_path
        # Set outlets for asset-based scheduling
        if outlets is not None:
            self.outlets = outlets
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute the Sling replication process.
        
        Returns:
            Dict containing metadata about the replication
        """
        # Create temporary configuration files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            # Write Sling configuration
            config_content = {
                'source': self.source_config,
                'target': self.target_config,
                **self.replication_config
            }
            
            # Convert to Sling YAML format
            import yaml
            yaml_content = yaml.dump(config_content, default_flow_style=False)
            config_file.write(yaml_content)
            config_file_path = config_file.name
            
            # Log the configuration for debugging
            self.log.info("Generated Sling configuration:")
            self.log.info(yaml_content)
        
        try:
            # Create database directory if needed for DuckDB target
            if 'duckdb' in str(self.target_config.get('type', '')).lower():
                db_path = Path(self.target_config.get('instance', ''))
                if db_path.suffix == '.db':
                    db_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Execute Sling command
            self.log.info("Starting Sling replication...")
            self.log.info(f"Source: {self.source_config}")
            self.log.info(f"Target: {self.target_config}")
            self.log.info(f"Streams: {self.replication_config.get('streams', {})}")
            
            # Build Sling command - remove problematic --verbose flag
            cmd = [
                self.sling_binary_path,
                "run",
                config_file_path
            ]
            
            self.log.info(f"Executing Sling command: {' '.join(cmd)}")
            
            # Execute the command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse output for metadata
            output_lines = result.stdout.split('\n')
            result.stderr.split('\n')
            
            # Extract replication statistics
            replicated_tables = []
            total_rows = 0
            
            for line in output_lines:
                if 'rows' in line.lower() and 'replicated' in line.lower():
                    # Try to extract table and row information
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part.isdigit():
                            total_rows += int(part)
                            break
                
                # Extract table names from streams
                for stream_name in self.replication_config.get('streams', {}).keys():
                    if stream_name in line:
                        table_name = stream_name.replace('.', '_')
                        if table_name not in replicated_tables:
                            replicated_tables.append(table_name)
            
            metadata = {
                "replicated_tables": replicated_tables,
                "total_rows_estimated": total_rows,
                "source_type": self.source_config.get('type'),
                "target_type": self.target_config.get('type'),
                "streams": list(self.replication_config.get('streams', {}).keys()),
                "stdout": result.stdout,
                "stderr": result.stderr
            }
            
            # Log success
            self.log.info("Sling replication completed successfully")
            self.log.info(f"Replicated tables: {replicated_tables}")
            self.log.info(f"Output: {result.stdout}")
            
            if result.stderr:
                self.log.warning(f"Stderr: {result.stderr}")
            
        except subprocess.CalledProcessError as e:
            self.log.error(f"Sling replication failed: {e}")
            self.log.error(f"Stdout: {e.stdout}")
            self.log.error(f"Stderr: {e.stderr}")
            raise
        
        finally:
            # Clean up temporary file
            Path(config_file_path).unlink(missing_ok=True)
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(
            key="sling_replication_metadata",
            value=metadata
        )
        
        return metadata


class SlingPostgresToDuckDBOperator(SlingReplicationOperator):
    """
    Specialized Sling operator for PostgreSQL to DuckDB replication.
    Pre-configured for the specific use case in the birds pipeline.
    """
    
    def __init__(
        self,
        postgres_config: Dict[str, Any],
        duckdb_database_path: str,
        tables_to_sync: List[str],
        mode: str = "full-refresh",
        outlets: Optional[List] = None,
        **kwargs
    ):
        """
        Initialize the operator with PostgreSQL to DuckDB specific configuration.
        
        Args:
            postgres_config: PostgreSQL connection configuration
            duckdb_database_path: Path to the DuckDB database file
            tables_to_sync: List of tables to sync (e.g., ['public.tickets', 'public.events'])
            mode: Replication mode (default: 'full-refresh')
            outlets: List of assets this operator produces (for Airflow 3 asset scheduling)
        """
        # Build source config
        source_config = {
            "type": "postgres",
            **postgres_config
        }
        
        # Build target config
        target_config = {
            "type": "duckdb",
            "instance": duckdb_database_path
        }
        
        # Build replication config
        streams = {}
        for table in tables_to_sync:
            streams[table] = None  # Use default configuration
        
        replication_config = {
            "source": "postgres",
            "target": "duckdb", 
            "defaults": {
                "mode": mode,
                "object": "{stream_schema}_{stream_table}"
            },
            "streams": streams
        }
        
        super().__init__(
            source_config=source_config,
            target_config=target_config,
            replication_config=replication_config,
            outlets=outlets,
            **kwargs
        ) 