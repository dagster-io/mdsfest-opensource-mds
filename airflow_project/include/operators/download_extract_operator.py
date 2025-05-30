import time
import zipfile
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional
from pathlib import Path

import requests
from airflow.models import BaseOperator
from airflow.utils.context import Context


class DownloadAndExtractOperator(BaseOperator):
    """
    Replicates the functionality of the BirdChecklist Dagster component.
    Downloads ZIP files from a URL and extracts them with metadata logging.
    
    This operator mirrors the _download_and_extract_data function from Dagster
    and provides the same metadata output capabilities.
    """
    
    template_fields = ("source_url", "extract_path", "asset_name")
    
    def __init__(
        self,
        source_url: str,
        extract_path: str,
        asset_name: str,
        create_dirs: bool = True,
        outlets: Optional[List] = None,
        **kwargs
    ):
        """
        Initialize the operator.
        
        Args:
            source_url: URL to download the ZIP file from
            extract_path: Directory path where to extract the ZIP contents
            asset_name: Name of the asset for logging and metadata
            create_dirs: Whether to create the extraction directory if it doesn't exist
            outlets: List of assets this operator produces (for Airflow 3 asset scheduling)
        """
        super().__init__(**kwargs)
        self.source_url = source_url
        self.extract_path = extract_path
        self.asset_name = asset_name
        self.create_dirs = create_dirs
        # Set outlets for asset-based scheduling
        if outlets is not None:
            self.outlets = outlets
    
    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute the download and extraction process.
        
        Returns:
            Dict containing metadata about the extraction process
        """
        return self._download_and_extract_data(context)
    
    def _download_and_extract_data(self, context: Context) -> Dict[str, Any]:
        """
        Downloads a zip file from a URL and extracts its contents.
        Mirrors the functionality from the Dagster BirdChecklist component.
        
        Returns:
            Tuple of (extracted_filenames, elapsed_time)
        """
        # Create extract path
        extract_path = Path(self.extract_path)
        if self.create_dirs:
            extract_path.mkdir(parents=True, exist_ok=True)
        
        with NamedTemporaryFile(suffix=".zip") as f:
            start_time = time.time()
            
            # Download the file
            self.log.info(f"Downloading {self.asset_name} data from {self.source_url}")
            response = requests.get(self.source_url)
            self.log.info(f"Downloaded {len(response.content)} bytes")
            
            # Write to temporary file
            f.write(response.content)
            f.seek(0)
            
            # Extract the ZIP file
            with zipfile.ZipFile(f.name, "r") as zip_ref:
                extracted_names = zip_ref.namelist()
                zip_ref.extractall(extract_path)
                end_time = time.time()
                self.log.info(f"Extracted {self.asset_name} data to {extract_path}")
        
        elapsed_time = end_time - start_time
        
        # Create metadata dictionary (similar to Dagster asset metadata)
        metadata = {
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_time,
            "source_url": self.source_url,
            "asset_name": self.asset_name,
            "extract_path": str(extract_path)
        }
        
        # Log metadata for monitoring
        self.log.info(f"Extraction completed for {self.asset_name}:")
        self.log.info(f"  - Files extracted: {len(extracted_names)}")
        self.log.info(f"  - Elapsed time: {elapsed_time:.2f} seconds")
        self.log.info(f"  - Files: {extracted_names}")
        
        # Push metadata to XCom for downstream tasks
        context['task_instance'].xcom_push(
            key=f"{self.asset_name}_metadata",
            value=metadata
        )
        
        return metadata 