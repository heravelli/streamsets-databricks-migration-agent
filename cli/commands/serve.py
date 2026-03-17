import typer

from config.settings import settings

def serve(
    host: str = typer.Option(None, "--host", help="Host to bind to"),
    port: int = typer.Option(None, "--port", "-p", help="Port to listen on"),
    reload: bool = typer.Option(False, "--reload", help="Enable auto-reload (dev mode)"),
):
    """
    Start the human review web portal.

    Example:
        migrate serve --port 8080
        migrate serve --reload  # development mode
    """
    import uvicorn

    h = host or settings.review_host
    p = port or settings.review_port
    uvicorn.run(
        "review.app:create_app",
        host=h,
        port=p,
        reload=reload,
        factory=True,
    )
