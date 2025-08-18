"""Notebook operations - replaces bash notebook creation/execution."""

import time
from pathlib import Path
from typing import Optional, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import Language, ObjectType, ImportFormat
from databricks.sdk.service.jobs import SubmitTask, NotebookTask, Source
from databricks.sdk.errors import DatabricksError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from databricks_ops.client import DatabricksClientWrapper

console = Console()


class NotebookManager:
    """Manages Databricks notebooks and job execution."""
    
    def __init__(self, client: WorkspaceClient):
        self.client = client
        self.wrapper = DatabricksClientWrapper(client)
    
    def create_notebook_from_file(self, local_path: str, workspace_path: str) -> bool:
        """Create a notebook in workspace from local file."""
        try:
            path = Path(local_path)
            if not path.exists():
                console.print(f"✗ Notebook file not found: {local_path}")
                return False
            
            # Read notebook content
            with open(path, 'r') as f:
                if path.suffix == '.ipynb':
                    # Jupyter notebook
                    notebook_content = f.read()
                    language = Language.PYTHON
                else:
                    # Assume Python script
                    content = f.read()
                    # Convert to simple notebook format for upload
                    notebook_content = content
                    language = Language.PYTHON
            
            def upload_notebook():
                return self.client.workspace.upload(
                    path=workspace_path,
                    content=notebook_content.encode('utf-8'),
                    language=language,
                    overwrite=True,
                    format=ImportFormat.JUPYTER if path.suffix == '.ipynb' else ImportFormat.SOURCE
                )
            
            self.wrapper.execute_with_retry(upload_notebook)
            console.print(f"✓ Created notebook: {workspace_path}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to create notebook {workspace_path}: {e}")
            return False
    
    def run_notebook_job(self, notebook_path: str, table_name: str, pii_columns: str, 
                        batch_size: int, timeout_minutes: int = 15) -> bool:
        """Run a notebook as a serverless job using SDK methods."""
        try:
            # Use serverless compute with multi-task format
            run_name = f"Serverless_Tokenize_{table_name.replace('.', '_')}_{int(time.time())}"
            
            console.print(f"Running notebook: {notebook_path}")
            console.print(f"Batch size: {batch_size}")
            
            # Create submit task using SDK classes
            submit_task = SubmitTask(
                task_key="tokenize_task",
                notebook_task=NotebookTask(
                    notebook_path=notebook_path,
                    source=Source.WORKSPACE,
                    base_parameters={
                        "table_name": table_name,
                        "pii_columns": pii_columns,
                        "batch_size": str(batch_size)
                    }
                ),
                timeout_seconds=1800  # 30 minutes
            )
            
            # Submit job using SDK method
            def submit_job():
                return self.client.jobs.submit(
                    run_name=run_name,
                    tasks=[submit_task]
                )
            
            waiter = self.wrapper.execute_with_retry(submit_job)
            run_id = waiter.run_id
            console.print(f"✓ Started notebook run with ID: {run_id}")
            
            # Extract workspace ID for live logs URL
            workspace_id = self._extract_workspace_id()
            if workspace_id:
                host = self.client.config.host
                console.print(f"View live logs: {host}/jobs/runs/{run_id}?o={workspace_id}")
            
            # Wait for completion with progress
            console.print("Waiting for tokenization to complete...")
            return self._monitor_job_execution_sdk(run_id, timeout_minutes)
                
        except Exception as e:
            console.print(f"✗ Failed to run notebook job: {e}")
            return False
    
    def _extract_workspace_id(self) -> Optional[str]:
        """Extract workspace ID from hostname for live logs URL."""
        try:
            import re
            host = self.client.config.host
            # Extract from pattern: https://dbc-{workspace_id}-{suffix}.cloud.databricks.com
            match = re.search(r'dbc-([a-f0-9]+)-', host)
            return match.group(1) if match else None
        except:
            return None
    
    def _monitor_job_execution_sdk(self, run_id: int, timeout_minutes: int) -> bool:
        """Monitor job execution using SDK methods with same polling behavior."""
        max_wait_seconds = timeout_minutes * 60
        wait_time = 0
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Tokenization in progress...", total=None)
            
            while wait_time < max_wait_seconds:
                try:
                    # Use SDK method to get run status
                    def get_run_status():
                        return self.client.jobs.get_run(run_id)
                    
                    run = self.wrapper.execute_with_retry(get_run_status)
                    state = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else 'UNKNOWN'
                    
                    if state == "TERMINATED":
                        result_state = run.state.result_state.value if run.state and run.state.result_state else 'UNKNOWN'
                        if result_state == "SUCCESS":
                            progress.update(task, description="✅ Tokenization completed successfully")
                            console.print("✅ Tokenization completed successfully")
                            return True
                        else:
                            error_msg = run.state.state_message if run.state and run.state.state_message else f"Failed with result: {result_state}"
                            progress.update(task, description="❌ Tokenization failed")
                            console.print(f"❌ Tokenization failed with result: {result_state}")
                            console.print(f"Error: {error_msg}")
                            return False
                    
                    elif state in ["INTERNAL_ERROR", "FAILED", "TIMEDOUT", "CANCELED", "SKIPPED"]:
                        progress.update(task, description=f"❌ Tokenization run failed: {state}")
                        console.print(f"❌ Tokenization run failed with state: {state}")
                        return False
                    
                    else:
                        # Job still running - update progress
                        progress.update(task, description=f"Tokenization in progress... (state: {state})")
                        time.sleep(30)  # Poll every 30 seconds
                        wait_time += 30
                
                except Exception as e:
                    console.print(f"Error monitoring job: {e}")
                    time.sleep(30)
                    wait_time += 30
            
            # Timeout reached
            progress.update(task, description="❌ Tokenization timed out")
            console.print(f"❌ Tokenization timed out after {timeout_minutes} minutes")
            return False
    
    def delete_notebook(self, workspace_path: str) -> bool:
        """Delete a notebook from workspace."""
        try:
            # Check if notebook exists first to avoid retry confusion
            if not self.notebook_exists(workspace_path):
                console.print(f"✓ Notebook {workspace_path} doesn't exist")
                return True
            
            def delete():
                return self.client.workspace.delete(workspace_path, recursive=True)
            
            self.wrapper.execute_with_retry(delete)
            console.print(f"✓ Deleted notebook: {workspace_path}")
            return True
            
        except DatabricksError as e:
            console.print(f"✗ Failed to delete notebook {workspace_path}: {e}")
            return False
    
    def list_notebooks(self, workspace_path: str) -> List[str]:
        """List notebooks in a workspace directory."""
        try:
            objects = self.client.workspace.list(workspace_path)
            return [
                obj.path for obj in objects 
                if obj.object_type == ObjectType.NOTEBOOK
            ]
        except DatabricksError:
            return []
    
    def notebook_exists(self, workspace_path: str) -> bool:
        """Check if a notebook exists in workspace."""
        try:
            obj = self.client.workspace.get_status(workspace_path)
            return obj.object_type == ObjectType.NOTEBOOK
        except DatabricksError:
            return False
    
    def setup_tokenization_notebook(self, prefix: str) -> bool:
        """Setup and execute the tokenization notebook."""
        # Get template path relative to this module
        template_dir = Path(__file__).parent.parent / "templates"
        local_notebook_path = template_dir / "notebooks" / "notebook_tokenize_table.ipynb"
        # Use Shared folder path
        workspace_path = f"/Shared/{prefix}_tokenize_table"
        
        # Create the notebook
        if not self.create_notebook_from_file(str(local_notebook_path), workspace_path):
            return False
        
        return True
    
    def execute_tokenization_notebook(self, prefix: str, batch_size: int) -> bool:
        """Execute the tokenization notebook with parameters."""
        workspace_path = f"/Shared/{prefix}_tokenize_table"
        table_name = f"{prefix}_catalog.default.{prefix}_customer_data"
        pii_columns = "first_name,last_name,email,phone_number,address,date_of_birth"  # PII columns to tokenize
        
        console.print("Tokenizing PII data in sample table...")
        return self.run_notebook_job(
            notebook_path=workspace_path,
            table_name=table_name, 
            pii_columns=pii_columns,
            batch_size=batch_size
        )