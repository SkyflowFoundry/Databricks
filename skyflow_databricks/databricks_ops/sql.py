"""SQL execution - replaces bash execute_sql functionality."""

import time
from pathlib import Path
from typing import Dict, Optional, List, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, StatementResponse
from databricks.sdk.errors import DatabricksError
from rich.console import Console
from rich.table import Table
from databricks_ops.client import DatabricksClientWrapper

console = Console()


class SQLExecutor:
    """Executes SQL files and statements against Databricks."""
    
    def __init__(self, client: WorkspaceClient, warehouse_id: str):
        self.client = client
        self.warehouse_id = warehouse_id
        self.wrapper = DatabricksClientWrapper(client)
    
    def apply_substitutions(self, sql: str, substitutions: Dict[str, str]) -> str:
        """Apply variable substitutions to SQL content."""
        if not substitutions:
            return sql
        
        for key, value in substitutions.items():
            sql = sql.replace(f"${{{key}}}", str(value))
        
        return sql
    
    def execute_statement(self, sql: str, timeout: int = 300) -> Optional[StatementResponse]:
        """Execute a single SQL statement."""
        try:
            def execute():
                return self.client.statement_execution.execute_statement(
                    warehouse_id=self.warehouse_id,
                    statement=sql,
                    wait_timeout="30s"
                )
            
            response = self.wrapper.execute_with_retry(execute)
            
            # Wait for completion if needed
            if response.status.state in [StatementState.PENDING, StatementState.RUNNING]:
                def check_completion():
                    result = self.client.statement_execution.get_statement(response.statement_id)
                    return result.status.state in [StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED]
                
                if self.wrapper.wait_for_completion("SQL execution", check_completion, timeout):
                    response = self.client.statement_execution.get_statement(response.statement_id)
            
            if response.status.state == StatementState.SUCCEEDED:
                return response
            else:
                error_msg = response.status.error.message if response.status.error else "Unknown error"
                console.print(f"✗ SQL execution failed: {error_msg}")
                return None
                
        except DatabricksError as e:
            console.print(f"✗ SQL execution error: {e}")
            return None
    
    def execute_sql_file(self, file_path: str, substitutions: Optional[Dict[str, str]] = None) -> bool:
        """Execute SQL from a file with variable substitutions."""
        # If path is relative, look in templates directory
        if not Path(file_path).is_absolute():
            template_dir = Path(__file__).parent.parent / "templates"
            path = template_dir / file_path
        else:
            path = Path(file_path)
        
        if not path.exists():
            console.print(f"✗ SQL file not found: {path}")
            return False
        
        console.print(f"Executing SQL file: {path.name}")
        
        try:
            with open(path, 'r') as f:
                sql_content = f.read()
            
            # Apply substitutions
            if substitutions:
                sql_content = self.apply_substitutions(sql_content, substitutions)
            
            # Split into individual statements (simple approach)
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            success = True
            for i, statement in enumerate(statements):
                console.print(f"  Executing statement {i+1}/{len(statements)}")
                result = self.execute_statement(statement)
                
                if result is None:
                    success = False
                    console.print(f"✗ Failed to execute statement {i+1}")
                    break
                else:
                    console.print(f"  ✓ Statement {i+1} completed")
            
            if success:
                console.print(f"✓ Successfully executed {path.name}")
            
            return success
            
        except Exception as e:
            console.print(f"✗ Error reading/executing {file_path}: {e}")
            return False
    
    def execute_query_with_results(self, sql: str, max_rows: int = 100) -> Optional[List[Dict[str, Any]]]:
        """Execute a query and return results."""
        response = self.execute_statement(sql)
        
        if response and response.result and response.result.data_array:
            # Convert to list of dictionaries
            columns = [col.name for col in response.manifest.schema.columns] if response.manifest else []
            results = []
            
            for row in response.result.data_array[:max_rows]:
                row_dict = {columns[i]: row[i] for i in range(len(columns))} if columns else {}
                results.append(row_dict)
            
            return results
        
        return None
    
    def verify_table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        sql = f"DESCRIBE TABLE {table_name}"
        result = self.execute_statement(sql)
        return result is not None
    
    def verify_function_exists(self, function_name: str) -> bool:
        """Check if a function exists."""
        sql = f"DESCRIBE FUNCTION {function_name}"
        result = self.execute_statement(sql)
        return result is not None
    
    def get_table_row_count(self, table_name: str) -> Optional[int]:
        """Get row count for a table."""
        sql = f"SELECT COUNT(*) as count FROM {table_name}"
        results = self.execute_query_with_results(sql)
        
        if results and len(results) > 0:
            count_value = results[0].get('count', 0)
            # Convert to int if it's a string
            return int(count_value) if count_value is not None else 0
        
        return None
    
    def show_table_sample(self, table_name: str, limit: int = 5) -> None:
        """Display a sample of table data."""
        sql = f"SELECT * FROM {table_name} LIMIT {limit}"
        results = self.execute_query_with_results(sql, max_rows=limit)
        
        if results:
            table = Table(title=f"Sample data from {table_name}")
            
            # Add columns
            if results:
                for column in results[0].keys():
                    table.add_column(column)
                
                # Add rows
                for row in results:
                    table.add_row(*[str(value) for value in row.values()])
            
            console.print(table)
        else:
            console.print(f"No data found in {table_name}")