from pathlib import Path
import typer
from rich.console import Console
from rich.table import Table

from config.settings import settings
from models.migration import PipelineRecord
from parser.pipeline_parser import scan_directory, PipelineParseError
from state.state_manager import StateManager

console = Console()


def ingest(
    pipelines_dir: Path = typer.Argument(..., help="Directory containing StreamSets JSON/YAML exports"),
    team: str = typer.Option(..., "--team", "-t", help="Team name to associate these pipelines with"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse and report without writing state"),
):
    """
    Parse StreamSets pipeline exports and register them in the migration state.

    Example:
        migrate ingest ./data/pipelines/team_alpha/ --team team_alpha
    """
    if not pipelines_dir.exists():
        console.print(f"[red]Directory not found: {pipelines_dir}[/red]")
        raise typer.Exit(1)

    state_manager = StateManager(settings.state_file)
    results = scan_directory(pipelines_dir)

    if not results:
        console.print(f"[yellow]No pipeline files found in {pipelines_dir}[/yellow]")
        raise typer.Exit(0)

    table = Table(title=f"Pipeline Ingest: {team}", show_lines=True)
    table.add_column("File", style="cyan")
    table.add_column("Pipeline ID", style="dim")
    table.add_column("Title")
    table.add_column("Stages", justify="right")
    table.add_column("Status")

    success_count = 0
    error_count = 0

    for path, result in results:
        rel_path = str(path.relative_to(pipelines_dir) if path.is_relative_to(pipelines_dir) else path)
        if isinstance(result, Exception):
            table.add_row(rel_path, "-", "-", "-", f"[red]ERROR: {result}[/red]")
            error_count += 1
        else:
            pipeline = result
            table.add_row(
                rel_path,
                pipeline.pipeline_id[:16] + "...",
                pipeline.title,
                str(len(pipeline.stages)),
                "[green]OK[/green]",
            )
            if not dry_run:
                record = PipelineRecord(
                    pipeline_id=pipeline.pipeline_id,
                    pipeline_title=pipeline.title,
                    team_name=team,
                    source_file_path=str(path.absolute()),
                )
                state_manager.register_pipeline(record, team)
            success_count += 1

    console.print(table)

    if dry_run:
        console.print(f"\n[yellow]Dry run: {success_count} pipelines parsed, not written to state[/yellow]")
    else:
        console.print(f"\n[green]✓ Ingested {success_count} pipelines for team '{team}'[/green]")

    if error_count:
        console.print(f"[red]✗ {error_count} files failed to parse[/red]")
