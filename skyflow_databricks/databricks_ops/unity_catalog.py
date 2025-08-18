"""Unity Catalog operations - replaces bash UC connection setup."""

from typing import Dict, Optional, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ConnectionType
from databricks.sdk.errors import DatabricksError
from rich.console import Console
from databricks_ops.client import DatabricksClientWrapper

console = Console()


class UnityCatalogManager:
    """Manages Unity Catalog resources for Skyflow integration."""
    
    def __init__(self, client: WorkspaceClient):
        self.client = client
        self.wrapper = DatabricksClientWrapper(client)
    
    def create_http_connection(self, name: str, host: str, base_path: str, 
                             secret_scope: str, secret_key: str) -> bool:
        """Create Unity Catalog HTTP connection."""
        try:
            # Check if connection already exists
            if self.wrapper.check_resource_exists(
                "connection", 
                lambda: self.client.connections.get(name)
            ):
                console.print(f"✓ Connection '{name}' already exists")
                return True
            
            # Create the connection
            def create_conn():
                return self.client.connections.create(
                    name=name,
                    connection_type=ConnectionType.HTTP,
                    options={
                        "host": host,
                        "port": "443",
                        "base_path": base_path
                    },
                    properties={
                        "bearer_token": f"secret('{secret_scope}', '{secret_key}')"
                    }
                )
            
            self.wrapper.execute_with_retry(create_conn)
            console.print(f"✓ Created HTTP connection: {name}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to create connection {name}: {e}")
            return False
    
    def create_catalog(self, name: str, comment: Optional[str] = None) -> bool:
        """Create Unity Catalog catalog."""
        try:
            if self.wrapper.check_resource_exists(
                "catalog",
                lambda: self.client.catalogs.get(name)
            ):
                console.print(f"✓ Catalog '{name}' already exists")
                return True
            
            def create_cat():
                return self.client.catalogs.create(
                    name=name,
                    comment=comment or f"Skyflow integration catalog - {name}"
                )
            
            self.wrapper.execute_with_retry(create_cat)
            console.print(f"✓ Created catalog: {name}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to create catalog {name}: {e}")
            return False
    
    def create_schema(self, catalog_name: str, schema_name: str = "default") -> bool:
        """Create schema in Unity Catalog."""
        full_name = f"{catalog_name}.{schema_name}"
        
        try:
            if self.wrapper.check_resource_exists(
                "schema",
                lambda: self.client.schemas.get(full_name)
            ):
                console.print(f"✓ Schema '{full_name}' already exists")
                return True
            
            def create_sch():
                return self.client.schemas.create(
                    name=schema_name,
                    catalog_name=catalog_name
                )
            
            self.wrapper.execute_with_retry(create_sch)
            console.print(f"✓ Created schema: {full_name}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to create schema {full_name}: {e}")
            return False
    
    def drop_catalog(self, name: str, force: bool = True) -> bool:
        """Drop Unity Catalog catalog and all contents."""
        try:
            if not self.wrapper.check_resource_exists(
                "catalog",
                lambda: self.client.catalogs.get(name)
            ):
                console.print(f"✓ Catalog '{name}' doesn't exist")
                return True
            
            def drop_cat():
                return self.client.catalogs.delete(name, force=force)
            
            self.wrapper.execute_with_retry(drop_cat)
            console.print(f"✓ Dropped catalog: {name}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to drop catalog {name}: {e}")
            return False
    
    def drop_connection(self, name: str) -> bool:
        """Drop Unity Catalog HTTP connection."""
        try:
            if not self.wrapper.check_resource_exists(
                "connection",
                lambda: self.client.connections.get(name)
            ):
                console.print(f"✓ Connection '{name}' doesn't exist")
                return True
            
            def drop_conn():
                return self.client.connections.delete(name)
            
            self.wrapper.execute_with_retry(drop_conn)
            console.print(f"✓ Dropped connection: {name}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to drop connection {name}: {e}")
            return False
    
    def setup_skyflow_connections(self, vault_url: str, vault_id: str) -> bool:
        """Setup both Skyflow HTTP connections."""
        success = True
        
        # Main Skyflow connection
        success &= self.create_http_connection(
            name="skyflow_conn",
            host=vault_url.replace("https://", "").replace("http://", ""),
            base_path="/v1/vaults",
            secret_scope="skyflow-secrets",
            secret_key="skyflow_pat_token"
        )
        
        return success
    
    def catalog_exists(self, name: str) -> bool:
        """Check if a catalog exists."""
        return self.wrapper.check_resource_exists(
            "catalog",
            lambda: self.client.catalogs.get(name)
        )
    
    def connection_exists(self, name: str) -> bool:
        """Check if a connection exists."""
        return self.wrapper.check_resource_exists(
            "connection",
            lambda: self.client.connections.get(name)
        )