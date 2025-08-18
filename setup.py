#!/usr/bin/env python3
"""
Skyflow Databricks Integration Setup Tool

Modern Python CLI using Databricks SDK for secure PII tokenization.
"""

import sys
import click
from pathlib import Path
from rich.console import Console
from rich.traceback import install

# Install rich traceback handler
install()
console = Console()

# Add the skyflow_databricks directory to Python path
skyflow_dir = Path(__file__).parent / "skyflow_databricks"
sys.path.insert(0, str(skyflow_dir))

from cli.commands import CreateCommand, DestroyCommand, VerifyCommand
from config.config import SetupConfig
from utils.logging import setup_logging


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.option('--config', '-c', default='.env.local', help='Configuration file path')
@click.pass_context
def cli(ctx, verbose, config):
    """Skyflow Databricks Integration Setup Tool."""
    
    # Setup logging
    log_level = "DEBUG" if verbose else "INFO"
    logger = setup_logging(log_level)
    
    # Store config in context
    ctx.ensure_object(dict)
    ctx.obj['config_file'] = config
    ctx.obj['logger'] = logger


@cli.command()
@click.argument('prefix')
@click.pass_context
def create(ctx, prefix):
    """Create a new Skyflow Databricks integration."""
    try:
        config = SetupConfig(ctx.obj['config_file'])
        command = CreateCommand(prefix, config)
        success = command.execute()
        sys.exit(0 if success else 1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument('prefix')
@click.pass_context
def destroy(ctx, prefix):
    """Destroy an existing Skyflow Databricks integration."""
    
    try:
        config = SetupConfig(ctx.obj['config_file'])
        command = DestroyCommand(prefix, config)
        success = command.execute()
        sys.exit(0 if success else 1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument('prefix')
@click.pass_context  
def recreate(ctx, prefix):
    """Recreate a Skyflow Databricks integration (destroy then create)."""
    
    try:
        config = SetupConfig(ctx.obj['config_file'])
        
        # Destroy first
        console.print("[bold blue]Phase 1: Destroying existing resources[/bold blue]")
        destroy_command = DestroyCommand(prefix, config)
        destroy_success = destroy_command.execute()
        
        if not destroy_success:
            console.print("[yellow]Warning: Destroy phase had some errors, continuing with create...[/yellow]")
        
        # Create new
        console.print("\n[bold blue]Phase 2: Creating new resources[/bold blue]")
        create_command = CreateCommand(prefix, config)
        create_success = create_command.execute()
        
        sys.exit(0 if create_success else 1)
        
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.argument('prefix')
@click.pass_context
def verify(ctx, prefix):
    """Verify an existing Skyflow Databricks integration."""
    try:
        config = SetupConfig(ctx.obj['config_file'])
        command = VerifyCommand(prefix, config)
        success = command.execute()
        sys.exit(0 if success else 1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)


@cli.command()
@click.pass_context
def config_test(ctx):
    """Test configuration and Databricks connection."""
    try:
        config = SetupConfig(ctx.obj['config_file'])
        console.print("[blue]Testing configuration...[/blue]")
        config.validate()
        
        # Test connection
        user = config.client.current_user.me()
        console.print(f"✓ Connected to Databricks as: {user.user_name}")
        console.print(f"✓ Workspace: {config.databricks.host}")
        console.print(f"✓ Warehouse ID: {config.databricks.warehouse_id}")
        
        console.print("[bold green]✓ Configuration test passed[/bold green]")
        
    except Exception as e:
        console.print(f"[red]Configuration test failed: {e}[/red]")
        sys.exit(1)


if __name__ == '__main__':
    cli()