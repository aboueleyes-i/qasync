from rich.console import Console
from rich.table import Table

from qasync.syncer.base import SyncResult

console = Console()


def print_sync_results(results: list[SyncResult]) -> None:
    table = Table(title="Sync Results", show_lines=False)
    table.add_column("Target", style="bold")
    table.add_column("Status")
    table.add_column("Files", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Error", style="dim")

    for r in sorted(results, key=lambda x: x.target_name):
        status = "[green]OK[/green]" if r.success else "[red]FAILED[/red]"
        files = str(r.file_count) if r.success else "-"
        duration = f"{r.duration_seconds}s" if r.duration_seconds > 0 else "-"
        error = r.error or ""
        table.add_row(r.target_name, status, files, duration, error)

    console.print()
    console.print(table)


def print_check_results(results: dict[str, tuple[bool, str]]) -> None:
    table = Table(title="Connectivity Check", show_lines=False)
    table.add_column("Target", style="bold")
    table.add_column("Status")
    table.add_column("Error", style="dim")

    for name, (reachable, error) in sorted(results.items()):
        if reachable:
            table.add_row(name, "[green]OK[/green]", "")
        else:
            table.add_row(name, "[red]UNREACHABLE[/red]", error)

    console.print()
    console.print(table)


def print_targets(targets: dict, groups: dict) -> None:
    t = Table(title="Configured Targets", show_lines=False)
    t.add_column("Name", style="bold")
    t.add_column("Type")
    t.add_column("Destination", style="dim")

    for name, cfg in sorted(targets.items()):
        dest = cfg.bucket or cfg.container or cfg.host or cfg.namenode or cfg.base_path
        t.add_row(name, cfg.type, dest)

    console.print()
    console.print(t)

    if groups:
        g = Table(title="Groups", show_lines=False)
        g.add_column("Name", style="bold")
        g.add_column("Members")
        for name, members in sorted(groups.items()):
            g.add_row(name, ", ".join(members))
        g.add_row("[dim]all[/dim]", "[dim](all configured targets)[/dim]")
        console.print()
        console.print(g)


def prompt_select_targets(targets: dict) -> list[str]:
    """Show numbered target list, let user pick by number. Returns selected names."""
    names = sorted(targets.keys())
    console.print("\n[bold]Select targets:[/bold]")
    for i, name in enumerate(names, 1):
        cfg = targets[name]
        console.print(f"  [cyan]{i}[/cyan]. {name} ({cfg.type})")
    console.print("  [cyan]a[/cyan]. All targets")

    while True:
        choice = console.input("\n[bold]Enter numbers (comma-separated) or 'a' for all:[/bold] ")
        choice = choice.strip()
        if choice.lower() == "a":
            return names
        try:
            indices = [int(x.strip()) for x in choice.split(",")]
            selected = [names[i - 1] for i in indices if 1 <= i <= len(names)]
            if selected:
                return selected
        except (ValueError, IndexError):
            pass
        console.print("[red]Invalid selection. Try again.[/red]")


def confirm_retry_failed(failed: list[SyncResult]) -> bool:
    """Ask user if they want to retry failed targets."""
    names = ", ".join(r.target_name for r in failed)
    console.print(f"\n[yellow]{len(failed)} target(s) failed:[/yellow] {names}")
    choice = console.input("[bold]Retry failed targets? [y/N]:[/bold] ")
    return choice.strip().lower() in ("y", "yes")
