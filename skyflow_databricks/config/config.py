"""Main configuration class for Databricks Skyflow integration."""

from typing import Dict, Optional
from pydantic import BaseModel, ValidationError
from databricks.sdk import WorkspaceClient
from config.env_loader import EnvLoader


class DatabricksConfig(BaseModel):
    """Databricks configuration model."""
    host: str
    token: str
    warehouse_id: str
    http_path: Optional[str] = None


class SkyflowConfig(BaseModel):
    """Skyflow configuration model."""
    vault_url: str
    vault_id: str
    pat_token: str
    table: str
    batch_size: int = 25  # Default batch size


class GroupConfig(BaseModel):
    """Group mapping configuration."""
    plain_text_groups: str = "auditor"
    masked_groups: str = "customer_service"
    redacted_groups: str = "marketing"


class SetupConfig:
    """Main configuration manager for Databricks Skyflow setup."""
    
    def __init__(self, env_file: str = ".env.local"):
        self.env_loader = EnvLoader(env_file)
        self._databricks_config: Optional[DatabricksConfig] = None
        self._skyflow_config: Optional[SkyflowConfig] = None
        self._group_config: Optional[GroupConfig] = None
        self._client: Optional[WorkspaceClient] = None
    
    @property
    def databricks(self) -> DatabricksConfig:
        """Get Databricks configuration."""
        if self._databricks_config is None:
            config_data = self.env_loader.get_databricks_config()
            try:
                self._databricks_config = DatabricksConfig(**config_data)
            except ValidationError as e:
                raise ValueError(f"Invalid Databricks configuration: {e}")
        return self._databricks_config
    
    @property
    def skyflow(self) -> SkyflowConfig:
        """Get Skyflow configuration.""" 
        if self._skyflow_config is None:
            config_data = self.env_loader.get_skyflow_config()
            try:
                self._skyflow_config = SkyflowConfig(**config_data)
            except ValidationError as e:
                raise ValueError(f"Invalid Skyflow configuration: {e}")
        return self._skyflow_config
    
    @property
    def groups(self) -> GroupConfig:
        """Get group configuration."""
        if self._group_config is None:
            config_data = self.env_loader.get_group_mappings()
            self._group_config = GroupConfig(**config_data)
        return self._group_config
    
    @property
    def client(self) -> WorkspaceClient:
        """Get authenticated Databricks client."""
        if self._client is None:
            self._client = WorkspaceClient(
                host=self.databricks.host,
                token=self.databricks.token
            )
        return self._client
    
    def validate(self) -> None:
        """Validate all configuration is present and correct."""
        validation = self.env_loader.validate_config()
        missing = [key for key, valid in validation.items() if not valid]
        
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        
        # Test Databricks connection
        try:
            self.client.current_user.me()
        except Exception as e:
            raise ValueError(f"Failed to authenticate with Databricks: {e}")
        
        print("✓ Configuration validated successfully")
    
    def get_substitutions(self, prefix: str) -> Dict[str, str]:
        """Get variable substitutions for SQL templates."""
        return {
            "PREFIX": prefix,
            "SKYFLOW_VAULT_URL": self.skyflow.vault_url,
            "SKYFLOW_VAULT_ID": self.skyflow.vault_id,
            "SKYFLOW_TABLE": self.skyflow.table,
            "PLAIN_TEXT_GROUPS": self.groups.plain_text_groups,
            "MASKED_GROUPS": self.groups.masked_groups,
            "REDACTED_GROUPS": self.groups.redacted_groups
        }