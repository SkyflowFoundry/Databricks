"""Secrets management - replaces bash secrets setup functionality."""

from typing import Dict, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ScopeBackendType
from databricks.sdk.errors import DatabricksError
from rich.console import Console
from databricks_ops.client import DatabricksClientWrapper

console = Console()


class SecretsManager:
    """Manages Databricks secrets and secret scopes."""
    
    def __init__(self, client: WorkspaceClient):
        self.client = client
        self.wrapper = DatabricksClientWrapper(client)
    
    def create_secret_scope(self, scope_name: str, backend_type: str = "DATABRICKS") -> bool:
        """Create a secret scope."""
        try:
            # Check if scope already exists
            existing_scopes = self.client.secrets.list_scopes()
            if any(scope.name == scope_name for scope in existing_scopes):
                console.print(f"✓ Secret scope '{scope_name}' already exists")
                return True
            
            # Map backend type
            backend = ScopeBackendType.DATABRICKS
            if backend_type.upper() == "AZURE_KEYVAULT":
                backend = ScopeBackendType.AZURE_KEYVAULT
            
            def create_scope():
                return self.client.secrets.create_scope(
                    scope=scope_name,
                    scope_backend_type=backend
                )
            
            self.wrapper.execute_with_retry(create_scope)
            console.print(f"✓ Created secret scope: {scope_name}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to create secret scope {scope_name}: {e}")
            return False
    
    def put_secret(self, scope_name: str, key: str, value: str) -> bool:
        """Put a secret value in the specified scope."""
        try:
            def put_secret_value():
                return self.client.secrets.put_secret(
                    scope=scope_name,
                    key=key,
                    string_value=value
                )
            
            self.wrapper.execute_with_retry(put_secret_value)
            console.print(f"✓ Set secret: {scope_name}/{key}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to set secret {scope_name}/{key}: {e}")
            return False
    
    def delete_secret_scope(self, scope_name: str) -> bool:
        """Delete a secret scope and all its secrets."""
        try:
            # Check if scope exists
            existing_scopes = self.client.secrets.list_scopes()
            if not any(scope.name == scope_name for scope in existing_scopes):
                console.print(f"✓ Secret scope '{scope_name}' doesn't exist")
                return True
            
            def delete_scope():
                return self.client.secrets.delete_scope(scope_name)
            
            self.wrapper.execute_with_retry(delete_scope)
            console.print(f"✓ Deleted secret scope: {scope_name}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to delete secret scope {scope_name}: {e}")
            return False
    
    def setup_skyflow_secrets(self, skyflow_config: Dict[str, str]) -> bool:
        """Setup all Skyflow-related secrets."""
        scope_name = "skyflow-secrets"
        
        # Create the scope
        if not self.create_secret_scope(scope_name):
            return False
        
        # Secret mappings
        secrets = {
            "skyflow_pat_token": skyflow_config["pat_token"],
            "skyflow_vault_id": skyflow_config["vault_id"],
            "skyflow_table": skyflow_config["table"],
            "skyflow_table_column": skyflow_config.get("table_column", "pii_values")  # Skyflow table column name
        }
        
        success = True
        for key, value in secrets.items():
            if not self.put_secret(scope_name, key, value):
                success = False
        
        return success
    
    def list_secrets_in_scope(self, scope_name: str) -> List[str]:
        """List all secret keys in a scope."""
        try:
            secrets = self.client.secrets.list_secrets(scope_name)
            return [secret.key for secret in secrets]
        except DatabricksError:
            return []
    
    def verify_secrets(self, scope_name: str, required_keys: List[str]) -> bool:
        """Verify that all required secrets exist in the scope."""
        existing_keys = self.list_secrets_in_scope(scope_name)
        missing_keys = [key for key in required_keys if key not in existing_keys]
        
        if missing_keys:
            console.print(f"✗ Missing secrets in {scope_name}: {', '.join(missing_keys)}")
            return False
        
        console.print(f"✓ All required secrets exist in {scope_name}")
        return True
    
    def secret_scope_exists(self, scope_name: str) -> bool:
        """Check if a secret scope exists."""
        return self.wrapper.check_resource_exists(
            "secret scope",
            lambda: self.client.secrets.list_secrets(scope_name)
        )