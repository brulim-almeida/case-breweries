"""
Pipeline Metadata Manager

This module handles saving and loading pipeline execution metadata
for monitoring and visualization purposes.
"""

import json
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PipelineMetadataManager:
    """
    Manager for pipeline execution metadata.
    
    Saves metadata about each pipeline run including:
    - Execution times per layer
    - Data volumes
    - Quality metrics
    - Validation results
    """
    
    def __init__(self, metadata_path: str = "/opt/airflow/lakehouse/metadata"):
        """
        Initialize metadata manager.
        
        Args:
            metadata_path: Path to store metadata files
        """
        self.metadata_path = Path(metadata_path)
        self.metadata_path.mkdir(parents=True, exist_ok=True)
        self.runs_file = self.metadata_path / "pipeline_runs.json"
        self.latest_file = self.metadata_path / "latest_run.json"
    
    def save_run_metadata(self, metadata: Dict[str, Any]) -> None:
        """
        Save metadata for a pipeline run.
        
        Args:
            metadata: Dictionary with run metadata
        """
        try:
            # Add timestamp if not present
            if 'execution_timestamp' not in metadata:
                metadata['execution_timestamp'] = datetime.now().isoformat()
            
            # Load existing runs
            all_runs = self._load_all_runs()
            
            # Append new run
            all_runs.append(metadata)
            
            # Keep only last 100 runs
            if len(all_runs) > 100:
                all_runs = all_runs[-100:]
            
            # Save all runs
            with open(self.runs_file, 'w') as f:
                json.dump(all_runs, f, indent=2)
            
            # Save as latest run
            with open(self.latest_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"✓ Saved pipeline metadata: {metadata.get('dag_run_id', 'unknown')}")
            
        except Exception as e:
            logger.error(f"Failed to save pipeline metadata: {e}")
    
    def get_latest_run(self) -> Optional[Dict[str, Any]]:
        """
        Get metadata for the most recent pipeline run.
        
        Returns:
            Dictionary with latest run metadata or None
        """
        try:
            if self.latest_file.exists():
                with open(self.latest_file, 'r') as f:
                    return json.load(f)
            return None
        except Exception as e:
            logger.error(f"Failed to load latest run metadata: {e}")
            return None
    
    def get_all_runs(self, limit: Optional[int] = None) -> list:
        """
        Get metadata for all pipeline runs.
        
        Args:
            limit: Maximum number of runs to return (most recent)
            
        Returns:
            List of run metadata dictionaries
        """
        runs = self._load_all_runs()
        if limit:
            return runs[-limit:]
        return runs
    
    def _load_all_runs(self) -> list:
        """Load all pipeline runs from file."""
        try:
            if self.runs_file.exists():
                with open(self.runs_file, 'r') as f:
                    return json.load(f)
            return []
        except Exception as e:
            logger.error(f"Failed to load pipeline runs: {e}")
            return []
    
    def get_execution_history(self, days: int = 30) -> 'pd.DataFrame':
        """
        Get execution history as DataFrame for analysis.
        
        Args:
            days: Number of days to include
            
        Returns:
            DataFrame with execution history
        """
        try:
            runs = self.get_all_runs()
            if not runs:
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(runs)
            
            # Parse timestamp
            df['execution_timestamp'] = pd.to_datetime(df['execution_timestamp'])
            
            # Filter by days
            cutoff = datetime.now() - timedelta(days=days)
            df = df[df['execution_timestamp'] >= cutoff]
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get execution history: {e}")
            return pd.DataFrame()
