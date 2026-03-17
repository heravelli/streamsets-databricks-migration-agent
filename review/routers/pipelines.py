import asyncio
import difflib
import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from agent.client_factory import create_client
from agent.migration_agent import MigrationAgent
from catalog.stage_catalog import StageCatalog
from config.settings import settings
from models.migration import PipelineStatus, ReviewComment, ReviewDecision
from parser.pipeline_parser import parse_file
from state.state_manager import StateManager

router = APIRouter(prefix="/pipelines", tags=["pipelines"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


class ReviewDecisionRequest(BaseModel):
    decision: ReviewDecision
    feedback: str = ""
    reviewer_id: str = "reviewer"
    line_references: list[int] = []


def _get_state() -> StateManager:
    return StateManager(settings.state_file)


def _get_agent() -> MigrationAgent:
    catalog = StageCatalog(settings.catalog_dir)
    client = create_client()
    return MigrationAgent(client, catalog)


@router.get("/")
async def list_pipelines(
    request: Request,
    team: Optional[str] = None,
    status: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
):
    state_manager = _get_state()
    state = state_manager.load()

    records = list(state.pipelines.values())
    if team:
        ids = set(state.team_index.get(team, []))
        records = [r for r in records if r.pipeline_id in ids]
    if status:
        try:
            status_enum = PipelineStatus(status)
            records = [r for r in records if r.status == status_enum]
        except ValueError:
            pass

    total = len(records)
    start = (page - 1) * page_size
    page_records = records[start : start + page_size]

    return templates.TemplateResponse(
        "pipeline_list.html",
        {
            "request": request,
            "records": page_records,
            "total": total,
            "page": page,
            "page_size": page_size,
            "team": team,
            "status": status,
            "teams": list(state.team_index.keys()),
        },
    )


@router.get("/{pipeline_id}/review")
async def review_pipeline(request: Request, pipeline_id: str):
    state_manager = _get_state()
    record = state_manager.get_pipeline(pipeline_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")

    # Load original pipeline JSON for display
    original_json = {}
    stage_summary = []
    try:
        pipeline = parse_file(Path(record.source_file_path))
        original_json = pipeline.raw
        catalog = StageCatalog(settings.catalog_dir)
        for stage in pipeline.stages:
            mapping = catalog.lookup(stage.stage_name)
            stage_summary.append({
                "instance_name": stage.instance_name,
                "stage_name": stage.short_name,
                "type": stage.type.value,
                "confidence": mapping.confidence.value if mapping else "unsupported",
                "databricks_label": mapping.databricks_label if mapping else "No mapping",
                "requires_review": mapping.requires_manual_review if mapping else True,
            })
    except Exception:
        pass

    return templates.TemplateResponse(
        "pipeline_review.html",
        {
            "request": request,
            "record": record,
            "original_json": json.dumps(original_json, indent=2),
            "stage_summary": stage_summary,
        },
    )


@router.post("/{pipeline_id}/decision")
async def submit_decision(
    pipeline_id: str,
    body: ReviewDecisionRequest,
    background_tasks: BackgroundTasks,
):
    state_manager = _get_state()
    record = state_manager.get_pipeline(pipeline_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")

    comment = ReviewComment(
        reviewer_id=body.reviewer_id,
        decision=body.decision,
        feedback=body.feedback,
        line_references=body.line_references,
    )

    history = list(record.review_history) + [comment]

    if body.decision == ReviewDecision.APPROVE:
        state_manager.update_pipeline(
            pipeline_id,
            status=PipelineStatus.APPROVED,
            review_history=history,
        )
        return {"status": "approved", "pipeline_id": pipeline_id}

    elif body.decision == ReviewDecision.REQUEST_CHANGES:
        state_manager.update_pipeline(
            pipeline_id,
            status=PipelineStatus.CHANGES_REQUESTED,
            review_history=history,
        )
        # Trigger re-migration in background
        background_tasks.add_task(_remigrate, pipeline_id, body.feedback)
        return {"status": "changes_requested", "pipeline_id": pipeline_id, "remigration": "queued"}

    elif body.decision == ReviewDecision.ESCALATE:
        state_manager.update_pipeline(
            pipeline_id,
            status=PipelineStatus.ESCALATED,
            review_history=history,
        )
        return {"status": "escalated", "pipeline_id": pipeline_id}

    elif body.decision == ReviewDecision.REJECT:
        state_manager.update_pipeline(
            pipeline_id,
            status=PipelineStatus.REJECTED,
            review_history=history,
        )
        return {"status": "rejected", "pipeline_id": pipeline_id}

    raise HTTPException(status_code=400, detail="Invalid decision")


async def _remigrate(pipeline_id: str, feedback: str):
    """Background task: re-run migration agent with reviewer feedback."""
    state_manager = _get_state()
    agent = _get_agent()

    record = state_manager.get_pipeline(pipeline_id)
    if not record:
        return

    try:
        state_manager.update_pipeline(pipeline_id, status=PipelineStatus.RE_MIGRATING)
        pipeline = parse_file(Path(record.source_file_path))
        artifact = await agent.migrate_pipeline(pipeline, feedback, record.current_artifact)

        artifact_dir = settings.output_dir / record.team_name / pipeline_id
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / artifact.filename).write_text(artifact.content)

        state_manager.update_pipeline(
            pipeline_id,
            status=PipelineStatus.IN_REVIEW,
            previous_artifact=record.current_artifact,
            current_artifact=artifact,
            migration_attempts=record.migration_attempts + 1,
        )
    except Exception as e:
        state_manager.update_pipeline(
            pipeline_id,
            status=PipelineStatus.FAILED,
            error_message=str(e),
        )


@router.get("/{pipeline_id}/diff")
async def get_diff(request: Request, pipeline_id: str):
    state_manager = _get_state()
    record = state_manager.get_pipeline(pipeline_id)
    if not record:
        raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")

    diff_lines = []
    if record.current_artifact and record.previous_artifact:
        diff_lines = list(
            difflib.unified_diff(
                record.previous_artifact.content.splitlines(keepends=True),
                record.current_artifact.content.splitlines(keepends=True),
                fromfile="previous",
                tofile="current",
            )
        )

    return templates.TemplateResponse(
        "diff_view.html",
        {
            "request": request,
            "record": record,
            "diff": "".join(diff_lines),
            "has_diff": bool(diff_lines),
        },
    )


@router.get("/{pipeline_id}/artifact")
async def download_artifact(pipeline_id: str):
    from fastapi.responses import PlainTextResponse
    state_manager = _get_state()
    record = state_manager.get_pipeline(pipeline_id)
    if not record or not record.current_artifact:
        raise HTTPException(status_code=404, detail="No artifact found")
    return PlainTextResponse(
        content=record.current_artifact.content,
        media_type="text/plain",
        headers={"Content-Disposition": f"attachment; filename={record.current_artifact.filename}"},
    )
