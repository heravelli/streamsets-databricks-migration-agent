from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from pathlib import Path

from config.settings import settings
from state.state_manager import StateManager
from state.progress_tracker import ProgressTracker

router = APIRouter(prefix="/teams", tags=["teams"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.get("/progress")
async def teams_progress(request: Request):
    state_manager = StateManager(settings.state_file)
    tracker = ProgressTracker(state_manager)
    teams = tracker.all_teams()
    overall = tracker.overall()
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "teams": teams, "overall": overall},
    )


@router.get("/progress/json")
async def teams_progress_json():
    state_manager = StateManager(settings.state_file)
    tracker = ProgressTracker(state_manager)
    return {
        "overall": tracker.overall(),
        "teams": [t.model_dump() for t in tracker.all_teams()],
    }
