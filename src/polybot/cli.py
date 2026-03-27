import typer
import uvicorn
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def start(port: int = 8000, reload: bool = True) -> None:
    """Startet den PolyBot Backend-Server."""
    console.print(
        "[bold green]🚀 PolyBot wird gestartet auf http://localhost:8000[/bold green]"
    )
    console.print(
        "[yellow]→ Frontend im zweiten Terminal starten: cd frontend && npm run dev[/yellow]"
    )
    uvicorn.run("polybot.main_fastapi:app", host="0.0.0.0", port=port, reload=reload)


def main() -> None:
    """Main entry point for the CLI."""
    app()


if __name__ == "__main__":
    app()
