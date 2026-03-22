#!/usr/bin/env python3
"""
CLI entry point for the ETL pipeline.

Usage:
    python run_etl.py --all              # Run all pipelines
    python run_etl.py --source crime     # Run a single pipeline
    python run_etl.py --source schools   # Run schools only
"""
import logging
import sys

import click

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl.log", mode="a"),
    ],
)


@click.command()
@click.option("--all", "run_all", is_flag=True, help="Run all ETL pipelines")
@click.option("--source", type=str, help="Run a specific ETL source")
def main(run_all: bool, source: str) -> None:
    """UK Home Dashboard — ETL Pipeline Runner."""
    from etl.pipeline import run_all as _run_all, run_single

    if not run_all and not source:
        click.echo("Specify --all or --source <name>. Use --help for options.")
        sys.exit(1)

    if run_all:
        click.echo("Running all ETL pipelines...\n")
        results = _run_all()
        click.echo("\n═══ ETL Results ═══")
        for name, status in results.items():
            icon = "✓" if "success" in status else "✗"
            click.echo(f"  {icon} {name}: {status}")
    else:
        click.echo(f"Running ETL for: {source}")
        try:
            status = run_single(source)
            click.echo(f"  Result: {status}")
        except ValueError as e:
            click.echo(f"  Error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
