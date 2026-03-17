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
        console.print("[yellow]No approved pipelines to export[/yellow]")
        raise typer.Exit(0)

    src_root = settings.output_dir
    tmp_dir = output_dir if fmt == "directory" else output_dir.parent / "_export_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    exported = 0
    for record in approved:
        src = src_root / record.team_name / record.pipeline_id
        if not src.exists():
            console.print(f"[yellow]Missing artifact for {record.pipeline_id}, skipping[/yellow]")
            continue
        dest = tmp_dir / record.team_name / record.pipeline_id
        dest.mkdir(parents=True, exist_ok=True)
        for f in src.iterdir():
            shutil.copy2(f, dest / f.name)
        exported += 1

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
