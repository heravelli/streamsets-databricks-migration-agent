import typer

from cli.commands.ingest import ingest
from cli.commands.migrate import run
from cli.commands.serve import serve
from cli.commands.status import status
from cli.commands.export import export

app = typer.Typer(
    name="migrate",
    help="StreamSets → Databricks Migration Agent",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)

app.command("ingest", help="Parse + register pipeline exports")(ingest)
app.command("run", help="Run migration agent on pipeline(s)")(run)
app.command("serve", help="Start the human review portal")(serve)
app.command("status", help="Show migration progress")(status)
app.command("export", help="Bundle approved artifacts for deployment")(export)


if __name__ == "__main__":
    app()
