import typer
from rich.console import Console
from rich.table import Table
from rich import box

from config.settings import settings
from models.migration import PipelineStatus
from state.state_manager import StateManager
from state.progress_tracker import ProgressTracker

console = Console()


def status(
    team: str = typer.Option(None, "--team", "-t", help="Show detail for one team"),
    fmt: str = typer.Option("table", "--format", "-f", help="Output format: table | json"),
):
    """
    Print migration progress report.

    Examples:
        migrate status
        migrate status --team team_alpha
    """
    state_manager = StateManager(settings.state_file)
    tracker = ProgressTracker(state_manager)

    if team:
        _show_team_detail(state_manager, team, fmt)
    else:
        _show_all_teams(tracker, fmt)


def _show_all_teams(tracker: ProgressTracker, fmt: str):
    overall = tracker.overall()
    teams = tracker.all_teams()

    if fmt == "json":
        import json
        console.print(json.dumps({"overall": overall, "teams": [t.model_dump() for t in teams]}, indent=2))
        return

    table = Table(
        title=f"Migration Progress — {overall['total']} pipelines, {overall['teams']} teams",
        box=box.ROUNDED,
        show_lines=False,
    )
    table.add_column("Team", style="cyan", min_width=20)
    table.add_column("Total", justify="right")
    table.add_column("Pending", justify="right", style="dim")
    table.add_column("In Review", justify="right", style="yellow")
    table.add_column("Approved", justify="right", style="green")
    table.add_column("Escalated", justify="right", style="magenta")
    table.add_column("Failed", justify="right", style="red")
    table.add_column("Done %", justify="right")

    for t in teams:
        pct = t.completion_pct
        pct_str = f"[green]{pct}%[/green]" if pct >= 80 else f"[yellow]{pct}%[/yellow]" if pct >= 40 else f"[red]{pct}%[/red]"
        table.add_row(
            t.team_name,
            str(t.total),
            str(t.pending),
            str(t.in_review),
            str(t.approved),
            str(t.escalated),
            str(t.failed),
            pct_str,
        )

    console.print(table)
    console.print(
        f"\nOverall: [green]{overall['approved']}[/green] / {overall['total']} approved "
        f"([bold]{overall['completion_pct']}%[/bold])"
    )


def _show_team_detail(state_manager: StateManager, team: str, fmt: str):
    state = state_manager.load()
    pipeline_ids = state.team_index.get(team, [])

    if not pipeline_ids:
        console.print(f"[yellow]Team '{team}' not found or has no pipelines[/yellow]")
        raise typer.Exit(1)

    records = [state.pipelines[pid] for pid in pipeline_ids if pid in state.pipelines]

    if fmt == "json":
        import json
        console.print(json.dumps([r.model_dump(mode="json") for r in records], indent=2, default=str))
        return

    table = Table(title=f"Team: {team}", box=box.ROUNDED)
    table.add_column("Pipeline ID", style="dim", max_width=20)
    table.add_column("Title", min_width=25)
    table.add_column("Status")
    table.add_column("Format", style="cyan")
    table.add_column("Confidence", justify="right")
    table.add_column("Attempts", justify="right")

    STATUS_COLORS = {
        PipelineStatus.APPROVED: "green",
        PipelineStatus.IN_REVIEW: "yellow",
        PipelineStatus.PENDING: "dim",
        PipelineStatus.FAILED: "red",
        PipelineStatus.ESCALATED: "magenta",
        PipelineStatus.REJECTED: "red",
    }

    for r in records:
        color = STATUS_COLORS.get(r.status, "white")
        conf = ""
        fmt_str = ""
        if r.current_artifact:
            conf_val = r.current_artifact.confidence_score
            conf = f"[green]{conf_val:.0%}[/green]" if conf_val >= 0.8 else f"[yellow]{conf_val:.0%}[/yellow]" if conf_val >= 0.5 else f"[red]{conf_val:.0%}[/red]"
            fmt_str = r.current_artifact.artifact_type.value

        table.add_row(
            r.pipeline_id[:20],
            r.pipeline_title[:35],
            f"[{color}]{r.status.value}[/{color}]",
            fmt_str,
            conf,
            str(r.migration_attempts),
        )

    console.print(table)
