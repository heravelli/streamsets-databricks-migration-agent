from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from review.routers import pipelines, teams


def create_app() -> FastAPI:
    app = FastAPI(
        title="StreamSets → Databricks Migration Review Portal",
        description="Human-in-the-loop review interface for migrated Databricks pipelines",
        version="0.1.0",
    )

    static_dir = Path(__file__).parent / "static"
    static_dir.mkdir(exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    app.include_router(pipelines.router)
    app.include_router(teams.router)

    @app.get("/")
    async def root():
        return RedirectResponse(url="/teams/progress")

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
