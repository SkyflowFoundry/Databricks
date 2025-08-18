"""Dashboard operations - replaces bash dashboard creation."""

import json
from pathlib import Path
from typing import Dict, Optional, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard
from databricks.sdk.errors import DatabricksError
from rich.console import Console
from databricks_ops.client import DatabricksClientWrapper

console = Console()


class DashboardManager:
    """Manages Databricks Lakeview dashboards."""
    
    def __init__(self, client: WorkspaceClient):
        self.client = client
        self.wrapper = DatabricksClientWrapper(client)
    
    def create_dashboard_from_file(self, local_path: str, dashboard_name: str, 
                                  warehouse_id: str, substitutions: Optional[Dict[str, str]] = None) -> Optional[str]:
        """Create a Lakeview dashboard from local JSON file using SDK methods."""
        try:
            path = Path(local_path)
            if not path.exists():
                console.print(f"✗ Dashboard file not found: {local_path}")
                return None
            
            # Read dashboard definition
            with open(path, 'r') as f:
                dashboard_content = f.read()
            
            # Apply substitutions to dashboard content
            if substitutions:
                for key, value in substitutions.items():
                    dashboard_content = dashboard_content.replace(f"${{{key}}}", str(value))
            
            # Parse JSON to validate
            try:
                dashboard_json = json.loads(dashboard_content)
            except json.JSONDecodeError as e:
                console.print(f"✗ Invalid JSON in dashboard file: {e}")
                return None
            
            # Delete existing dashboard if it exists (using SDK)
            self._delete_existing_dashboard_sdk(dashboard_name)
            
            # Create dashboard using SDK
            dashboard = Dashboard(
                display_name=dashboard_name,
                warehouse_id=warehouse_id,
                serialized_dashboard=json.dumps(dashboard_json),  # JSON encode dashboard content
                parent_path="/Shared"
            )
            
            def create_dashboard():
                return self.client.lakeview.create(dashboard)
            
            result = self.wrapper.execute_with_retry(create_dashboard)
            
            if result.dashboard_id:
                console.print(f"✓ Created dashboard: {dashboard_name}")
                host = self.client.config.host
                dashboard_url = f"{host}/sql/dashboardsv3/{result.dashboard_id}"
                console.print(f"  Dashboard URL: {dashboard_url}")
                return dashboard_url
            else:
                console.print(f"✗ Could not extract dashboard ID from response")
                return None
            
        except Exception as e:
            console.print(f"✗ Error creating dashboard: {e}")
            return None
    
    def _delete_existing_dashboard_sdk(self, dashboard_name: str) -> None:
        """Delete existing dashboard with the same name using SDK."""
        try:
            # Find existing dashboard by name using existing SDK method
            existing_id = self.find_dashboard_by_name(dashboard_name)
            if existing_id:
                console.print(f"Deleting existing dashboard: {dashboard_name}")
                self.delete_dashboard(existing_id)
        except Exception:
            # Don't fail if we can't delete existing - just continue
            pass
    
    def delete_dashboard(self, dashboard_id: str) -> bool:
        """Delete a dashboard by ID."""
        try:
            def delete():
                return self.client.lakeview.trash(dashboard_id)
            
            self.wrapper.execute_with_retry(delete)
            console.print(f"✓ Deleted dashboard: {dashboard_id}")
            return True
            
        except DatabricksError as e:
            if "not found" in str(e).lower():
                console.print(f"✓ Dashboard {dashboard_id} doesn't exist")
                return True
            console.print(f"✗ Failed to delete dashboard {dashboard_id}: {e}")
            return False
    
    def list_dashboards(self) -> list:
        """List all dashboards."""
        try:
            dashboards = self.client.lakeview.list()
            return [
                {
                    "id": d.dashboard_id,
                    "name": d.display_name,
                    "warehouse_id": d.warehouse_id
                }
                for d in dashboards
            ]
        except DatabricksError as e:
            console.print(f"✗ Failed to list dashboards: {e}")
            return []
    
    def find_dashboard_by_name(self, name: str) -> Optional[str]:
        """Find dashboard ID by name."""
        dashboards = self.list_dashboards()
        for dashboard in dashboards:
            if dashboard["name"] == name:
                return dashboard["id"]
        return None
    
    def setup_customer_insights_dashboard(self, prefix: str, warehouse_id: str) -> Optional[str]:
        """Setup the customer insights dashboard for the specified prefix."""
        # Get template path relative to this module
        template_dir = Path(__file__).parent.parent / "templates"
        dashboard_file = template_dir / "dashboards" / "customer_insights_dashboard.lvdash.json"
        dashboard_name = f"{prefix}_customer_insights_dashboard"
        
        # Prepare substitutions
        substitutions = {
            "PREFIX": prefix,
            f"{prefix.upper()}_CATALOG": f"{prefix}_catalog",
            f"{prefix.upper()}_CUSTOMER_DATASET": f"{prefix}_customer_data"
        }
        
        # Check if dashboard already exists
        existing_id = self.find_dashboard_by_name(dashboard_name)
        if existing_id:
            console.print(f"✓ Dashboard '{dashboard_name}' already exists")
            dashboard_url = f"{self.client.config.host}/sql/dashboardsv3/{existing_id}"
            console.print(f"  Dashboard URL: {dashboard_url}")
            return dashboard_url
        
        # Create new dashboard
        return self.create_dashboard_from_file(
            str(dashboard_file), 
            dashboard_name, 
            warehouse_id, 
            substitutions
        )
    
    def update_dashboard_warehouse(self, dashboard_id: str, warehouse_id: str) -> bool:
        """Update the warehouse used by a dashboard."""
        try:
            def update():
                return self.client.lakeview.update(
                    dashboard_id=dashboard_id,
                    warehouse_id=warehouse_id
                )
            
            self.wrapper.execute_with_retry(update)
            console.print(f"✓ Updated dashboard warehouse to: {warehouse_id}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to update dashboard warehouse: {e}")
            return False
    
    def publish_dashboard(self, dashboard_id: str) -> bool:
        """Publish a dashboard."""
        try:
            def publish():
                return self.client.lakeview.publish(dashboard_id)
            
            self.wrapper.execute_with_retry(publish)
            console.print(f"✓ Published dashboard: {dashboard_id}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to publish dashboard: {e}")
            return False