"""Environment configuration loader for Databricks Skyflow integration."""

import os
from pathlib import Path
from typing import Dict, Optional, Any
from dotenv import load_dotenv


class EnvLoader:
    """Loads and processes environment variables from .env.local file."""
    
    def __init__(self, env_file: str = ".env.local"):
        self.env_file = env_file
        self._load_env_file()
    
    def _load_env_file(self) -> None:
        """Load environment file if it exists."""
        env_path = Path(self.env_file)
        if env_path.exists():
            print(f"Loading configuration from {self.env_file}...")
            load_dotenv(env_path)
        else:
            print(f"Warning: {self.env_file} not found - using environment variables only")
    
    def get_databricks_config(self) -> Dict[str, Optional[str]]:
        """Extract Databricks configuration from environment."""
        hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        host = f"https://{hostname}" if hostname else None
        
        # Extract warehouse ID from HTTP path
        http_path = os.getenv("DATABRICKS_HTTP_PATH")
        warehouse_id = None
        if http_path and "/warehouses/" in http_path:
            warehouse_id = http_path.split("/warehouses/")[-1]
        
        return {
            "host": host,
            "token": os.getenv("DATABRICKS_PAT_TOKEN"),
            "warehouse_id": warehouse_id,
            "http_path": http_path
        }
    
    def get_skyflow_config(self) -> Dict[str, Any]:
        """Extract Skyflow configuration from environment."""
        return {
            "vault_url": os.getenv("SKYFLOW_VAULT_URL"),
            "vault_id": os.getenv("SKYFLOW_VAULT_ID"),
            "pat_token": os.getenv("SKYFLOW_PAT_TOKEN"),
            "table": os.getenv("SKYFLOW_TABLE"),
            "table_column": os.getenv("SKYFLOW_TABLE_COLUMN"),
            "batch_size": os.getenv("SKYFLOW_BATCH_SIZE")
        }
    
    def get_group_mappings(self) -> Dict[str, Optional[str]]:
        """Extract group mappings for detokenization."""
        return {
            "plain_text_groups": os.getenv("PLAIN_TEXT_GROUPS"),
            "masked_groups": os.getenv("MASKED_GROUPS"),
            "redacted_groups": os.getenv("REDACTED_GROUPS")
        }
    
    def validate_config(self) -> Dict[str, bool]:
        """Validate that required configuration is present."""
        databricks = self.get_databricks_config()
        skyflow = self.get_skyflow_config()
        
        return {
            "databricks_host": databricks["host"] is not None,
            "databricks_token": databricks["token"] is not None,
            "warehouse_id": databricks["warehouse_id"] is not None,
            "skyflow_vault_url": skyflow["vault_url"] is not None,
            "skyflow_vault_id": skyflow["vault_id"] is not None,
            "skyflow_pat_token": skyflow["pat_token"] is not None,
            "skyflow_table": skyflow["table"] is not None
        }