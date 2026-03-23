import shutil
import zipfile
from pathlib import Path
import typer
from rich.console import Console

from config.settings import settings
from models.migration import PipelineStatus
from state.state_manager import StateManager

console = Console()


def export(
    output_dir: Path = typer.Argument(..., help="Destination directory for exported artifacts"),
    team: str = typer.Option(None, "--team", "-t", help="Export only pipelines from this team"),
    fmt: str = typer.Option("directory", "--format", "-f", help="Output format: directory | zip"),
):
    """
    Export approved pipeline artifacts for deployment.

    Examples:
        migrate export ./deploy/
        migrate export ./deploy/ --team team_alpha
        migrate export ./bundle.zip --format zip
    """
    state_manager = StateManager(settings.state_file)
    state = state_manager.load()

    pipeline_ids = list(state.pipelines.keys())
    if team:
        pipeline_ids = state.team_index.get(team, [])
        if not pipeline_ids:
            console.print(f"[yellow]Team '{team}' not found[/yellow]")
            raise typer.Exit(1)

    approved = [
        state.pipelines[pid]
        for pid in pipeline_ids
        if pid in state.pipelines and state.pipelines[pid].status == PipelineStatus.APPROVED
    ]

    if not approved:
        console.print(
            "[yellow]No approved pipelines to export.[/yellow]\n"
            "[dim]Pipelines must be approved via the review portal before export.\n"
            "Run [bold]uv run migrate status[/bold] to check pipeline states.\n"
            "Run [bold]uv run migrate serve[/bold] then open http://localhost:8000 to approve.[/dim]"
        )
        raise typer.Exit(0)

    src_root = settings.output_dir
    console.print(f"[dim]Source artifacts root: {src_root.resolve()}[/dim]")

    # Guard: destination must not be the same as (or inside) the source output dir
    try:
        output_dir.resolve().relative_to(src_root.resolve())
        console.print(
            f"[red]Export destination cannot be inside the output directory ({src_root}).\n"
            f"Choose a different path, e.g.:[/red]\n"
            f"  migrate export /data/deploy/"
        )
        raise typer.Exit(1)
    except ValueError:
        pass  # destination is outside src_root — safe to proceed

    tmp_dir = output_dir if fmt == "directory" else output_dir.parent / "_export_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    exported = 0
    skipped = 0
    for record in approved:
        src = src_root / record.team_name / record.pipeline_id
        if not src.exists():
            console.print(
                f"[yellow]⚠ Missing artifact directory for {record.pipeline_id} "
                f"(expected: {src}) — skipping[/yellow]"
            )
            skipped += 1
            continue
        dest = tmp_dir / record.team_name / record.pipeline_id
        dest.mkdir(parents=True, exist_ok=True)
        # Only copy files (not subdirs) to avoid shutil.copy2 failing on directories
        for f in src.iterdir():
            if f.is_file():
                shutil.copy2(f, dest / f.name)
        exported += 1

    if skipped:
        console.print(f"[yellow]⚠ {skipped} pipeline(s) had no artifact on disk — were they migrated?[/yellow]")

    if fmt == "zip":
        zip_path = output_dir
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in tmp_dir.rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(tmp_dir))
        shutil.rmtree(tmp_dir)
        console.print(f"[green]✓ Exported {exported} pipelines → {zip_path}[/green]")
    else:
        console.print(f"[green]✓ Exported {exported} pipelines → {output_dir}[/green]")
