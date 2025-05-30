import subprocess
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context


class DbtOperator(BaseOperator):
    """
    Executes dbt commands.
    Replicates the functionality of Dagster dbt assets.
    """
    
    template_fields = ("dbt_project_dir", "dbt_command", "models", "target")
    
    def __init__(
        self,
        dbt_project_dir: str,
        dbt_command: str = "run",
        models: Optional[List[str]] = None,
        target: Optional[str] = None,
        dbt_binary_path: str = "dbt",
        outlets: Optional[List] = None,
        **kwargs
    ):
        """
        Initialize the operator.
        
        Args:
            dbt_project_dir: Path to the dbt project directory
            dbt_command: dbt command to execute (run, build, test, etc.)
            models: List of specific models to run (optional)
            target: dbt target profile to use (optional)
            dbt_binary_path: Path to the dbt binary
            outlets: List of assets this operator produces (for Airflow 3 asset scheduling)
        """
        super().__init__(**kwargs)
        self.dbt_project_dir = dbt_project_dir
        self.dbt_command = dbt_command
        self.models = models or []
        self.target = target
        self.dbt_binary_path = dbt_binary_path
        # Set outlets for asset-based scheduling
        if outlets is not None:
            self.outlets = outlets
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute the dbt command.
        
        Returns:
            Dict containing metadata about the dbt run
        """
        # Build dbt command
        cmd = [self.dbt_binary_path, self.dbt_command]
        
        # Add project directory
        cmd.extend(["--project-dir", self.dbt_project_dir])
        
        # Add target if specified
        if self.target:
            cmd.extend(["--target", self.target])
        
        # Add models if specified
        if self.models:
            cmd.extend(["--models"] + self.models)
        
        # Add verbose flag for better logging
        cmd.append("--verbose")
        
        self.log.info(f"Executing dbt command: {' '.join(cmd)}")
        
        try:
            # Execute the command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                cwd=self.dbt_project_dir
            )
            
            # Parse dbt output for metadata
            output_lines = result.stdout.split('\n')
            result.stderr.split('\n')
            
            # Extract run statistics
            models_run = []
            models_failed = []
            models_skipped = []
            
            for line in output_lines:
                if "RUN " in line or "PASS " in line:
                    # Extract model name from dbt output
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part in ["RUN", "PASS"]:
                            if i + 1 < len(parts):
                                model_name = parts[i + 1]
                                if model_name not in models_run:
                                    models_run.append(model_name)
                            break
                elif "FAIL " in line or "ERROR " in line:
                    # Extract failed model name
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part in ["FAIL", "ERROR"]:
                            if i + 1 < len(parts):
                                model_name = parts[i + 1]
                                if model_name not in models_failed:
                                    models_failed.append(model_name)
                            break
                elif "SKIP " in line:
                    # Extract skipped model name
                    parts = line.split()
                    for i, part in enumerate(parts):
                        if part == "SKIP":
                            if i + 1 < len(parts):
                                model_name = parts[i + 1]
                                if model_name not in models_skipped:
                                    models_skipped.append(model_name)
                            break
            
            metadata = {
                "command": self.dbt_command,
                "models_run": models_run,
                "models_failed": models_failed,
                "models_skipped": models_skipped,
                "total_models": len(models_run) + len(models_failed) + len(models_skipped),
                "success_count": len(models_run),
                "failure_count": len(models_failed),
                "skip_count": len(models_skipped),
                "stdout": result.stdout,
                "stderr": result.stderr,
                "return_code": result.returncode
            }
            
            # Log results
            self.log.info(f"dbt {self.dbt_command} completed successfully")
            self.log.info(f"Models run: {len(models_run)}")
            self.log.info(f"Models failed: {len(models_failed)}")
            self.log.info(f"Models skipped: {len(models_skipped)}")
            
            if models_run:
                self.log.info(f"Successfully run models: {', '.join(models_run)}")
            if models_failed:
                self.log.error(f"Failed models: {', '.join(models_failed)}")
            if models_skipped:
                self.log.info(f"Skipped models: {', '.join(models_skipped)}")
            
            # Log full output for debugging
            self.log.debug(f"dbt stdout: {result.stdout}")
            if result.stderr:
                self.log.warning(f"dbt stderr: {result.stderr}")
            
        except subprocess.CalledProcessError as e:
            self.log.error(f"dbt {self.dbt_command} failed: {e}")
            self.log.error(f"Stdout: {e.stdout}")
            self.log.error(f"Stderr: {e.stderr}")
            
            metadata = {
                "command": self.dbt_command,
                "models_run": [],
                "models_failed": [],
                "models_skipped": [],
                "total_models": 0,
                "success_count": 0,
                "failure_count": 0,
                "skip_count": 0,
                "stdout": e.stdout,
                "stderr": e.stderr,
                "return_code": e.returncode,
                "error": str(e)
            }
            
            # Push metadata even on failure for debugging
            context['task_instance'].xcom_push(
                key=f"dbt_{self.dbt_command}_metadata",
                value=metadata
            )
            
            raise
        
        # Push metadata to XCom
        context['task_instance'].xcom_push(
            key=f"dbt_{self.dbt_command}_metadata",
            value=metadata
        )
        
        return metadata


class DbtRunOperator(DbtOperator):
    """
    Specialized dbt operator for running models.
    """
    
    def __init__(self, outlets: Optional[List] = None, **kwargs):
        kwargs['dbt_command'] = 'run'
        super().__init__(outlets=outlets, **kwargs)


class DbtBuildOperator(DbtOperator):
    """
    Specialized dbt operator for building models (run + test).
    """
    
    def __init__(self, outlets: Optional[List] = None, **kwargs):
        kwargs['dbt_command'] = 'build'
        super().__init__(outlets=outlets, **kwargs)


class DbtTestOperator(DbtOperator):
    """
    Specialized dbt operator for testing models.
    """
    
    def __init__(self, outlets: Optional[List] = None, **kwargs):
        kwargs['dbt_command'] = 'test'
        super().__init__(outlets=outlets, **kwargs) 