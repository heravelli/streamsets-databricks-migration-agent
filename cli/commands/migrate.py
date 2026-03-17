import asyncio
from pathlib import Path
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from agent.client_factory import create_client
from agent.migration_agent import MigrationAgent
from catalog.stage_catalog import StageCatalog
from config.settings import settings
from models.migration import PipelineStatus
from parser.pipeline_parser import parse_file
from state.state_manager import StateManager

console = Console()


async def _run_single(
    pipeline_id: str,
    agent: MigrationAgent,
    state_manager: StateManager,
    output_dir: Path,
) -> bool:
    record = state_manager.get_pipeline(pipeline_id)
    if record is None:
        console.print(f"[red]Pipeline {pipeline_id} not found in state[/red]")
        return False

    try:
        state_manager.update_pipeline(pipeline_id, status=PipelineStatus.MIGRATING)
        pipeline = parse_file(Path(record.source_file_path))

        # Pass prior artifact + feedback for re-migration
        feedback = None
        prior_artifact = None
        if record.review_history and record.current_artifact:
            last_review = record.review_history[-1]
            feedback = last_review.feedback
            prior_artifact = record.current_artifact

        artifact = await agent.migrate_pipeline(pipeline, feedback, prior_artifact)

        # Write artifact to output directory
        artifact_dir = output_dir / record.team_name / pipeline_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / artifact.filename).write_text(artifact.content)
        (artifact_dir / "migration_metadata.json").write_text(
            artifact.model_dump_json(indent=2, exclude={"content"})
        )

        state_manager.update_pipeline(
            pipeline_id,
            status=PipelineStatus.IN_REVIEW,
            current_artifact=artifact,
            migration_attempts=record.migration_attempts + 1,
            previous_artifact=record.current_artifact,
        )
        return True

    except Exception as e:
        state_manager.update_pipeline(
            pipeline_id,
            status=PipelineStatus.FAILED,
            error_message=str(e),
        )
        console.print(f"[red]✗ {pipeline_id}: {e}[/red]")
        return False


async def _run_batch(pipeline_ids: list[str], agent: MigrationAgent, state_manager: StateManager):
    output_dir = settings.output_dir
    sem = asyncio.Semaphore(settings.agent_concurrency)

    async def run_with_sem(pid: str):
        async with sem:
            return await _run_single(pid, agent, state_manager, output_dir)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Migrating {len(pipeline_ids)} pipelines...", total=len(pipeline_ids))
        results = []
        for coro in asyncio.as_completed([run_with_sem(pid) for pid in pipeline_ids]):
            result = await coro
            results.append(result)
            progress.advance(task)

    success = sum(1 for r in results if r)
    failed = len(results) - success
    console.print(f"\n[green]✓ {success} migrated[/green], [red]✗ {failed} failed[/red]")


def run(
    pipeline_id: str = typer.Option(None, "--pipeline-id", help="Run for a single pipeline"),
    team: str = typer.Option(None, "--team", "-t", help="Run for all pending pipelines in a team"),
    all_pending: bool = typer.Option(False, "--all", help="Run for ALL pending pipelines"),
    concurrency: int = typer.Option(None, "--concurrency", "-c", help="Override default concurrency"),
):
    """
    Run the migration agent on pipeline(s).

    Examples:
        migrate run --pipeline-id abc-123
        migrate run --team team_alpha --concurrency 5
        migrate run --all --concurrency 10
    """
    if concurrency:
        settings.agent_concurrency = concurrency

    state_manager = StateManager(settings.state_file)
    catalog = StageCatalog(settings.catalog_dir)
    client = create_client()
    agent = MigrationAgent(client, catalog)

    if pipeline_id:
        pipeline_ids = [pipeline_id]
    elif team:
        records = state_manager.get_pending_for_team(team)
        pipeline_ids = [r.pipeline_id for r in records]
        if not pipeline_ids:
            console.print(f"[yellow]No pending pipelines for team '{team}'[/yellow]")
            raise typer.Exit(0)
    elif all_pending:
        records = state_manager.get_pipelines_by_status(PipelineStatus.PENDING)
        pipeline_ids = [r.pipeline_id for r in records]
        if not pipeline_ids:
            console.print("[yellow]No pending pipelines found[/yellow]")
            raise typer.Exit(0)
    else:
        console.print("[red]Specify --pipeline-id, --team, or --all[/red]")
        raise typer.Exit(1)

    console.print(f"Running migration agent on {len(pipeline_ids)} pipeline(s) "
                  f"(concurrency={settings.agent_concurrency})")

    asyncio.run(_run_batch(pipeline_ids, agent, state_manager))
