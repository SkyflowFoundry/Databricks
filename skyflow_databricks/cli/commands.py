"""CLI command implementations for Databricks Skyflow integration."""

import time
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from config.config import SetupConfig
from databricks_ops.unity_catalog import UnityCatalogManager
from databricks_ops.secrets import SecretsManager
from databricks_ops.sql import SQLExecutor
from databricks_ops.notebooks import NotebookManager
from databricks_ops.dashboards import DashboardManager
from utils.validation import validate_prefix, validate_required_files

console = Console()


class BaseCommand:
    """Base class for all commands."""
    
    def __init__(self, prefix: str, config: Optional[SetupConfig] = None):
        self.prefix = prefix
        self.config = config or SetupConfig()
        
        # Validate prefix
        is_valid, error = validate_prefix(prefix)
        if not is_valid:
            raise ValueError(f"Invalid prefix: {error}")
    
    def validate_environment(self):
        """Validate environment and configuration."""
        try:
            self.config.validate()
        except ValueError as e:
            console.print(f"[red]Configuration error: {e}[/red]")
            raise


class CreateCommand(BaseCommand):
    """Implementation of 'create' command."""
    
    def execute(self) -> bool:
        """Execute the create command."""
        console.print(Panel.fit(
            f"Creating Skyflow Databricks Integration: [bold]{self.prefix}[/bold]",
            style="green"
        ))
        
        try:
            # Always destroy first to ensure clean state
            console.print(f"[dim]Cleaning up any existing '{self.prefix}' resources...[/dim]")
            destroy_command = DestroyCommand(self.prefix, self.config)
            destroy_command.execute()  # Don't fail if destroy has issues
            
            # Validate environment
            self.validate_environment()
            
            # Check required files exist
            required_files = [
                "sql/setup/create_sample_table.sql", 
                "sql/setup/create_uc_connections.sql",
                "sql/setup/setup_uc_connections_api.sql",
                "sql/setup/apply_column_masks.sql",
                "notebooks/notebook_tokenize_table.ipynb",
                "dashboards/customer_insights_dashboard.lvdash.json"
            ]
            
            files_exist, missing = validate_required_files(required_files)
            if not files_exist:
                console.print(f"[red]Missing required files: {', '.join(missing)}[/red]")
                return False
            
            # Initialize managers
            uc_manager = UnityCatalogManager(self.config.client)
            secrets_manager = SecretsManager(self.config.client)
            sql_executor = SQLExecutor(self.config.client, self.config.databricks.warehouse_id)
            notebook_manager = NotebookManager(self.config.client)
            dashboard_manager = DashboardManager(self.config.client)
            
            # Get substitutions
            substitutions = self.config.get_substitutions(self.prefix)
            
            # Step 1: Create Unity Catalog resources
            console.print("\n[bold blue]Step 1: Setting up Unity Catalog[/bold blue]")
            if not self._setup_unity_catalog(uc_manager):
                return False
            
            # Step 2: Setup secrets
            console.print("\n[bold blue]Step 2: Setting up secrets[/bold blue]")
            if not self._setup_secrets(secrets_manager):
                return False
            
            # Step 3: Create connections
            console.print("\n[bold blue]Step 3: Creating HTTP connections[/bold blue]")
            if not self._setup_connections(sql_executor, substitutions):
                return False
            
            # Step 4: Create sample data
            console.print("\n[bold blue]Step 4: Creating sample table[/bold blue]")
            if not self._create_sample_data(sql_executor, substitutions):
                return False
            
            # Step 5: Create tokenization notebook
            console.print("\n[bold blue]Step 5: Creating tokenization notebook[/bold blue]")
            if not self._create_tokenization_notebook(notebook_manager):
                return False
            
            # Step 6: Verify functions before tokenization
            console.print("\n[bold blue]Step 6: Verifying functions[/bold blue]")
            if not self._verify_functions(sql_executor, substitutions):
                console.print("[yellow]⚠ Function verification failed - continuing[/yellow]")
            
            # Step 7: Execute tokenization (BEFORE applying column masks!)
            console.print("\n[bold blue]Step 7: Running tokenization[/bold blue]")
            tokenization_success = self._execute_tokenization(notebook_manager)
            if not tokenization_success:
                console.print("[yellow]⚠ Tokenization failed - continuing with setup[/yellow]")
            
            # Step 8: Apply column masks AFTER tokenization (correct order!)
            console.print("\n[bold blue]Step 8: Applying column masks to tokenized data[/bold blue]")
            if tokenization_success:  # Only apply masks if tokenization succeeded
                functions_success = self._setup_functions(sql_executor, substitutions)
                if not functions_success:
                    console.print("[yellow]⚠ Column masks failed - continuing without them[/yellow]")
            else:
                console.print("[yellow]⚠ Skipping column masks - tokenization failed[/yellow]")
            
            # Step 9: Create dashboard
            console.print("\n[bold blue]Step 9: Creating dashboard[/bold blue]")
            dashboard_url = self._create_dashboard(dashboard_manager)
            
            # Success summary
            self._print_success_summary(dashboard_url)
            return True
            
        except Exception as e:
            console.print(f"[red]Setup failed: {e}[/red]")
            return False
    
    def _setup_unity_catalog(self, uc_manager: UnityCatalogManager) -> bool:
        """Setup Unity Catalog resources."""
        catalog_name = f"{self.prefix}_catalog"
        
        success = uc_manager.create_catalog(catalog_name)
        success &= uc_manager.create_schema(catalog_name, "default")
        
        return success
    
    def _setup_secrets(self, secrets_manager: SecretsManager) -> bool:
        """Setup secret scope and secrets."""
        skyflow_config = {
            "pat_token": self.config.skyflow.pat_token,
            "vault_id": self.config.skyflow.vault_id,
            "table": self.config.skyflow.table
        }
        
        return secrets_manager.setup_skyflow_secrets(skyflow_config)
    
    def _setup_connections(self, sql_executor: SQLExecutor, substitutions: dict) -> bool:
        """Setup HTTP connections using SQL."""
        # Create connections using SQL file
        success = sql_executor.execute_sql_file(
            "sql/setup/create_uc_connections.sql",
            substitutions
        )
        
        if success:
            # Execute additional connection setup SQL (detokenization functions)
            success &= sql_executor.execute_sql_file(
                "sql/setup/setup_uc_connections_api.sql",
                substitutions
            )
        
        return success
    
    def _create_sample_data(self, sql_executor: SQLExecutor, substitutions: dict) -> bool:
        """Create sample table and data."""
        success = sql_executor.execute_sql_file(
            "sql/setup/create_sample_table.sql",
            substitutions
        )
        
        if success:
            # Check table exists first without counting rows (table might be empty initially)
            table_name = f"{self.prefix}_catalog.default.{self.prefix}_customer_data"
            if sql_executor.verify_table_exists(table_name):
                console.print(f"  ✓ Created table: {table_name}")
                row_count = sql_executor.get_table_row_count(table_name)
                if row_count is not None and row_count > 0:
                    console.print(f"  ✓ Table has {row_count} rows")
                else:
                    console.print(f"  ✓ Table created (empty)")
        
        return success
    
    def _setup_functions(self, sql_executor: SQLExecutor, substitutions: dict) -> bool:
        """Setup detokenization functions and column masks."""
        return sql_executor.execute_sql_file(
            "sql/setup/apply_column_masks.sql",
            substitutions
        )
    
    def _create_tokenization_notebook(self, notebook_manager: NotebookManager) -> bool:
        """Create the tokenization notebook."""
        try:
            return notebook_manager.setup_tokenization_notebook(self.prefix)
        except Exception as e:
            console.print(f"✗ Notebook creation failed: {e}")
            return False
    
    def _verify_functions(self, sql_executor: SQLExecutor, substitutions: dict) -> bool:
        """Verify Unity Catalog functions exist."""
        try:
            # Add 5 second delay for function creation
            console.print("Verifying function creation...")
            time.sleep(5)
            
            console.print("Verifying Unity Catalog detokenization functions...")
            success = sql_executor.execute_sql_file("sql/verify/verify_functions.sql", substitutions)
            if success:
                console.print("✓ Unity Catalog conditional detokenization functions verified")
            return success
        except Exception as e:
            console.print(f"✗ Function verification failed: {e}")
            return False
    
    def _execute_tokenization(self, notebook_manager: NotebookManager) -> bool:
        """Execute the tokenization notebook."""
        try:
            # Get batch size from config
            batch_size = getattr(self.config.skyflow, 'batch_size', 25)
            return notebook_manager.execute_tokenization_notebook(self.prefix, batch_size)
        except Exception as e:
            console.print(f"✗ Tokenization execution failed: {e}")
            return False
    
    def _create_dashboard(self, dashboard_manager: DashboardManager) -> Optional[str]:
        """Create the customer insights dashboard."""
        return dashboard_manager.setup_customer_insights_dashboard(
            self.prefix,
            self.config.databricks.warehouse_id
        )
    
    def _print_success_summary(self, dashboard_url: Optional[str]):
        """Print success summary with resources created."""
        console.print("\n" + "="*60)
        console.print(Panel.fit(
            f"[bold green]✓ Setup Complete: {self.prefix}[/bold green]",
            style="green"
        ))
        
        # Resources table
        table = Table(title="Resources Created")
        table.add_column("Resource", style="cyan")
        table.add_column("Name", style="green")
        
        table.add_row("Unity Catalog", f"{self.prefix}_catalog")
        table.add_row("Sample Table", f"{self.prefix}_customer_data")
        table.add_row("Secrets Scope", "skyflow-secrets")
        table.add_row("HTTP Connection", "skyflow_conn")
        table.add_row("Tokenization Notebook", f"{self.prefix}_tokenize_table")
        
        if dashboard_url:
            table.add_row("Dashboard", f"{self.prefix}_customer_insights_dashboard")
        
        console.print(table)
        
        if dashboard_url:
            console.print(f"\n[bold]Dashboard URL:[/bold] {dashboard_url}")
        
        console.print("\n[bold]Next Steps:[/bold]")
        console.print("1. Test role-based access by running queries as different users")
        console.print("2. Explore the dashboard to see detokenization in action")
        console.print("3. Use the SQL functions in your own queries and applications")


class DestroyCommand(BaseCommand):
    """Implementation of 'destroy' command."""
    
    def execute(self) -> bool:
        """Execute the destroy command."""
        console.print(Panel.fit(
            f"Destroying Skyflow Databricks Integration: [bold]{self.prefix}[/bold]",
            style="red"
        ))
        
        try:
            self.validate_environment()
            
            # Initialize managers
            uc_manager = UnityCatalogManager(self.config.client)
            secrets_manager = SecretsManager(self.config.client)
            notebook_manager = NotebookManager(self.config.client)
            dashboard_manager = DashboardManager(self.config.client)
            sql_executor = SQLExecutor(self.config.client, self.config.databricks.warehouse_id)
            
            # Track successful and failed deletions for validation
            successful_deletions = []
            failed_deletions = []
            
            # Step 1: Delete dashboard
            console.print("\n[bold blue]Step 1: Removing dashboard[/bold blue]")
            dashboard_name = f"{self.prefix}_customer_insights_dashboard"
            dashboard_id = dashboard_manager.find_dashboard_by_name(dashboard_name)
            if dashboard_id:
                if dashboard_manager.delete_dashboard(dashboard_id):
                    successful_deletions.append(f"Dashboard: {dashboard_name}")
                    # Validate deletion
                    if dashboard_manager.find_dashboard_by_name(dashboard_name):
                        failed_deletions.append(f"Dashboard: {dashboard_name} (still exists)")
                else:
                    failed_deletions.append(f"Dashboard: {dashboard_name}")
            else:
                console.print(f"✓ Dashboard '{dashboard_name}' doesn't exist")
                successful_deletions.append(f"Dashboard: {dashboard_name} (didn't exist)")
            
            # Step 2: Delete notebook
            console.print("\n[bold blue]Step 2: Removing notebook[/bold blue]")
            # Use Shared folder path
            notebook_path = f"/Shared/{self.prefix}_tokenize_table"
            if notebook_manager.delete_notebook(notebook_path):
                successful_deletions.append(f"Notebook: {notebook_path}")
                # Note: Validation handled in delete_notebook method
            # Note: delete_notebook already handles "doesn't exist" as success
            
            # Step 3: Remove column masks before dropping functions/table
            console.print("\n[bold blue]Step 3: Removing column masks[/bold blue]")
            catalog_name = f"{self.prefix}_catalog"
            substitutions = {"PREFIX": self.prefix}
            if uc_manager.catalog_exists(catalog_name):
                if sql_executor.execute_sql_file("sql/destroy/remove_column_masks.sql", substitutions):
                    successful_deletions.append("Column masks removed")
                else:
                    console.print("✓ Column masks removal skipped (may not exist)")
                    successful_deletions.append("Column masks (skipped)")
            else:
                console.print("✓ Column masks removal skipped (catalog doesn't exist)")
                successful_deletions.append("Column masks (catalog didn't exist)")
            
            # Step 4: Drop functions before dropping catalog
            console.print("\n[bold blue]Step 4: Dropping Unity Catalog functions[/bold blue]")
            catalog_name = f"{self.prefix}_catalog"
            if uc_manager.catalog_exists(catalog_name):
                if sql_executor.execute_sql_file("sql/destroy/drop_functions.sql", substitutions):
                    successful_deletions.append("Unity Catalog functions")
                    # Note: Function validation skipped - functions are dropped before catalog
                else:
                    failed_deletions.append("Unity Catalog functions")
            else:
                console.print(f"✓ Catalog '{catalog_name}' doesn't exist, skipping function cleanup")
                successful_deletions.append("Functions (catalog didn't exist)")
            
            # Step 5: Drop table
            console.print("\n[bold blue]Step 5: Dropping sample table[/bold blue]")
            if uc_manager.catalog_exists(catalog_name):
                if sql_executor.execute_sql_file("sql/destroy/drop_table.sql", substitutions):
                    successful_deletions.append("Sample table")
                    # Note: Table validation skipped - table is dropped before catalog
                else:
                    failed_deletions.append("Sample table")
            else:
                successful_deletions.append("Sample table (catalog didn't exist)")
            
            # Step 6: Delete catalog
            console.print("\n[bold blue]Step 6: Removing Unity Catalog[/bold blue]")
            if uc_manager.drop_catalog(catalog_name):
                successful_deletions.append(f"Catalog: {catalog_name}")
                # Validate catalog deletion
                if uc_manager.catalog_exists(catalog_name):
                    failed_deletions.append(f"Catalog: {catalog_name} (still exists)")
            else:
                failed_deletions.append(f"Catalog: {catalog_name}")
            
            # Step 7: Delete connection (single consolidated Skyflow connection)
            console.print("\n[bold blue]Step 7: Cleaning up connections[/bold blue]")
            conn_name = "skyflow_conn"
            if uc_manager.drop_connection(conn_name):
                successful_deletions.append(f"Connection: {conn_name}")
                # Validate connection deletion
                if uc_manager.connection_exists(conn_name):
                    failed_deletions.append(f"Connection: {conn_name} (still exists)")
            # Note: If connection doesn't exist, drop_connection already handles this gracefully
            
            # Step 8: Delete secrets (only if no other catalogs using them)
            console.print("\n[bold blue]Step 8: Cleaning up secrets[/bold blue]")
            if secrets_manager.delete_secret_scope("skyflow-secrets"):
                successful_deletions.append("Secret scope: skyflow-secrets")
                # Validate secret scope deletion
                if secrets_manager.secret_scope_exists("skyflow-secrets"):
                    failed_deletions.append("Secret scope: skyflow-secrets (still exists)")
            else:
                failed_deletions.append("Secret scope: skyflow-secrets")
            
            # Print comprehensive validation summary
            self._print_destroy_summary(successful_deletions, failed_deletions)
            
            # Return success only if all deletions succeeded and were validated
            return len(failed_deletions) == 0
            
        except Exception as e:
            console.print(f"[red]Destroy failed: {e}[/red]")
            return False
    
    def _print_destroy_summary(self, successful: list, failed: list):
        """Print a detailed summary of destroy operation results."""
        console.print("\n" + "="*60)
        console.print("[bold]Destroy Summary[/bold]")
        
        if successful:
            console.print(f"\n[bold green]Successfully deleted ({len(successful)}):[/bold green]")
            for item in successful:
                console.print(f"  ✓ {item}")
        
        if failed:
            console.print(f"\n[bold red]Failed to delete ({len(failed)}):[/bold red]")
            for item in failed:
                console.print(f"  ✗ {item}")
            console.print("\n[yellow]Warning: Some resources could not be deleted or verified. Manual cleanup may be required.[/yellow]")
            console.print(Panel.fit(
                f"[bold red]⚠ Cleanup completed with {len(failed)} errors[/bold red]",
                style="yellow"
            ))
        else:
            console.print(Panel.fit(
                f"[bold green]✓ All resources successfully deleted and validated[/bold green]",
                style="green"
            ))


class VerifyCommand(BaseCommand):
    """Implementation of 'verify' command."""
    
    def execute(self) -> bool:
        """Execute the verify command."""
        console.print(Panel.fit(
            f"Verifying Skyflow Databricks Integration: [bold]{self.prefix}[/bold]",
            style="blue"
        ))
        
        try:
            self.validate_environment()
            
            sql_executor = SQLExecutor(self.config.client, self.config.databricks.warehouse_id)
            
            # Verify table exists and has data
            table_name = f"{self.prefix}_catalog.default.{self.prefix}_customer_data"
            table_exists = sql_executor.verify_table_exists(table_name)
            
            if table_exists:
                row_count = sql_executor.get_table_row_count(table_name)
                console.print(f"✓ Table exists with {row_count} rows")
                sql_executor.show_table_sample(table_name)
            else:
                console.print(f"✗ Table {table_name} does not exist")
                return False
            
            # Verify functions exist
            function_name = f"{self.prefix}_catalog.default.{self.prefix}_skyflow_conditional_detokenize"
            function_exists = sql_executor.verify_function_exists(function_name)
            
            if function_exists:
                console.print(f"✓ Function {function_name} exists")
            else:
                console.print(f"✗ Function {function_name} does not exist")
                return False
            
            console.print(Panel.fit(
                f"[bold green]✓ Verification Complete: {self.prefix}[/bold green]",
                style="green"
            ))
            
            return True
            
        except Exception as e:
            console.print(f"[red]Verification failed: {e}[/red]")
            return False