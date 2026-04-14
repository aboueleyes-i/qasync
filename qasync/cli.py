import shutil
from pathlib import Path

import click
from rich.console import Console

from qasync.config import ConfigError, QaSyncConfig, load_config, resolve_targets, save_config
from qasync.output import (
    confirm_retry_failed,
    print_check_results,
    print_sync_results,
    print_targets,
    prompt_select_targets,
)
from qasync.runner import run_check, run_clean, run_sync
from qasync.syncer.registry import get_syncer_class

console = Console()
DEFAULT_CONFIG = str(Path.home() / ".qasync" / "config.yaml")


def _check_rclone():
    if not shutil.which("rclone"):
        raise click.ClickException(
            "rclone is not installed. Install it from https://rclone.org/install/"
        )


def _build_syncers(targets):
    syncers = []
    for t in targets:
        cls = get_syncer_class(t.type)
        syncer = cls(t.name, t.__dict__)
        syncers.append(syncer)
    return syncers


def _load_config_or_exit(config_path: str) -> QaSyncConfig:
    try:
        return load_config(Path(config_path))
    except ConfigError as e:
        raise click.ClickException(str(e))


def _parse_remote_paths(remote_path_str, target_names):
    """Parse --remote-path value into {target_name: path} dict.

    Formats:
      "/qa/run1"              -> applies to all targets
      "s3=/qa/run1,gcs=/stg"  -> per-target paths
    """
    if not remote_path_str:
        return {}

    # Check if it's per-target (contains =) or a single path for all
    if "=" in remote_path_str:
        result = {}
        for part in remote_path_str.split(","):
            part = part.strip()
            if "=" not in part:
                raise click.ClickException(
                    f"Invalid --remote-path format: '{part}'. Use target=/path"
                )
            name, path = part.split("=", 1)
            name = name.strip()
            if name not in target_names:
                raise click.ClickException(f"Unknown target in --remote-path: {name}")
            result[name] = path.strip()
        return result
    else:
        # Single path applies to all targets
        return {name: remote_path_str for name in target_names}


def _resolve_or_pick(cfg, targets_str, group):
    """Resolve targets from flags, or show interactive picker if neither given."""
    target_names = targets_str.split(",") if targets_str else None
    if target_names or group:
        try:
            return resolve_targets(cfg, target_names=target_names, group=group)
        except ConfigError as e:
            raise click.ClickException(str(e))

    # Interactive picker
    if not cfg.targets:
        raise click.ClickException("No targets configured. Run 'qasync add <name>' first.")
    selected_names = prompt_select_targets(cfg.targets)
    return [cfg.targets[n] for n in selected_names]


@click.group()
def main():
    """qasync -- QA test data sync tool."""


@main.command()
@click.argument("source", type=click.Path(exists=False))
@click.option("--targets", "-t", default=None, help="Comma-separated target names")
@click.option("--group", "-g", default=None, help="Target group name")
@click.option("--dry-run", is_flag=True, help="Preview only, no transfers")
@click.option("--parallel", "-p", default=3, help="Max concurrent uploads")
@click.option(
    "--flat", is_flag=True,
    help="Upload contents directly into base_path (no subdirectory created)",
)
@click.option(
    "--remote-path", "-r", default=None,
    help="Override remote path. Single path or per-target: s3=/qa/run1,gcs=/stg",
)
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def upload(source, targets, group, dry_run, parallel, flat, remote_path, config):
    """Upload a directory to storage targets.

    \b
    By default, creates a subdirectory named after SOURCE on the remote:
      qasync upload ./test-data --targets s3
      -> s3:bucket/base_path/test-data/

    \b
    With --flat, uploads contents directly into base_path:
      qasync upload ./test-data --targets s3 --flat
      -> s3:bucket/base_path/

    \b
    Override remote path for all targets:
      qasync upload ./test-data --targets s3,gcs --remote-path /qa/2026-04-14
      -> s3:bucket/qa/2026-04-14/test-data/

    \b
    Override remote path per target:
      qasync upload ./test-data --targets s3,gcs -r s3=/qa/run1,gcs=/staging/run1
      -> s3:bucket/qa/run1/test-data/
      -> gcs:bucket/staging/run1/test-data/
    """
    source_path = Path(source)
    if not source_path.exists():
        raise click.ClickException(f"Source path does not exist: {source}")

    cfg = _load_config_or_exit(config)
    resolved = _resolve_or_pick(cfg, targets, group)

    has_rclone_targets = any(t.type != "hdfs" for t in resolved)
    if has_rclone_targets:
        _check_rclone()

    syncers = _build_syncers(resolved)
    parallel = cfg.defaults.get("parallel", parallel)

    # Parse --remote-path into a dict of {target_name: path}
    remote_paths = _parse_remote_paths(remote_path, [t.name for t in resolved])

    if dry_run:
        console.print("[yellow]DRY RUN -- no files will be transferred[/yellow]\n")

    results = run_sync(
        syncers, source_path, max_parallel=parallel,
        dry_run=dry_run, flat=flat, remote_paths=remote_paths,
    )
    print_sync_results(results)

    # Retry loop for failed targets
    failed = [r for r in results if not r.success]
    while failed and not dry_run:
        if not confirm_retry_failed(failed):
            break
        failed_names = {r.target_name for r in failed}
        retry_syncers = [s for s in syncers if s.name in failed_names]
        results = run_sync(
            retry_syncers, source_path, max_parallel=parallel,
            flat=flat, remote_paths=remote_paths,
        )
        print_sync_results(results)
        failed = [r for r in results if not r.success]

    if failed:
        raise SystemExit(1)


@main.command("list")
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def list_targets(config):
    """List configured targets and groups."""
    cfg = _load_config_or_exit(config)
    print_targets(cfg.targets, cfg.groups)


@main.command()
@click.option("--targets", "-t", default=None, help="Comma-separated target names")
@click.option("--group", "-g", default=None, help="Target group name")
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def check(targets, group, config):
    """Check target connectivity."""
    cfg = _load_config_or_exit(config)
    resolved = _resolve_or_pick(cfg, targets, group)
    syncers = _build_syncers(resolved)
    results = run_check(syncers)
    print_check_results(results)


@main.command()
@click.argument("remote_path")
@click.option("--targets", "-t", default=None, help="Comma-separated target names")
@click.option("--group", "-g", default=None, help="Target group name")
@click.option("--parallel", "-p", default=3, help="Max concurrent operations")
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def clean(remote_path, targets, group, parallel, config):
    """Delete test data from targets."""
    cfg = _load_config_or_exit(config)
    resolved = _resolve_or_pick(cfg, targets, group)
    syncers = _build_syncers(resolved)
    results = run_clean(syncers, remote_path, max_parallel=parallel)
    print_sync_results(results)


@main.command()
@click.argument("name")
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def add(name, config):
    """Add a new target interactively."""
    config_path = Path(config)
    try:
        cfg = load_config(config_path)
    except ConfigError:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        cfg = QaSyncConfig()

    from qasync.setup import add_target_interactive

    add_target_interactive(cfg, name, config_path)

    # Post-add connectivity check
    if name in cfg.targets:
        console.print("\n[bold]Verifying connectivity...[/bold]")
        target = cfg.targets[name]
        cls = get_syncer_class(target.type)
        syncer = cls(target.name, target.__dict__)
        reachable, error = syncer.check()
        if reachable:
            console.print(f"  [green]Connected to {name} successfully.[/green]")
        else:
            console.print(f"  [red]Could not reach {name}:[/red] {error}")
            console.print("  You can reconfigure with:")
            console.print(f"    qasync remove {name} && qasync add {name}")


@main.command()
@click.argument("name")
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def remove(name, config):
    """Remove a target."""
    config_path = Path(config)
    cfg = _load_config_or_exit(config)

    from qasync.setup import remove_target

    remove_target(cfg, name, config_path)


@main.group("group")
def group_cmd():
    """Manage target groups."""


@group_cmd.command("create")
@click.argument("name")
@click.option("--targets", "-t", required=True, help="Comma-separated target names")
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def group_create(name, targets, config):
    """Create a target group."""
    config_path = Path(config)
    cfg = _load_config_or_exit(config)

    members = targets.split(",")
    for m in members:
        if m not in cfg.targets:
            raise click.ClickException(f"Unknown target: {m}")

    cfg.groups[name] = members
    save_config(cfg, config_path)
    console.print(f"Group [bold]'{name}'[/bold] created: {', '.join(members)}")


@group_cmd.command("delete")
@click.argument("name")
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def group_delete(name, config):
    """Delete a target group."""
    config_path = Path(config)
    cfg = _load_config_or_exit(config)

    if name not in cfg.groups:
        raise click.ClickException(f"Group '{name}' not found")

    del cfg.groups[name]
    save_config(cfg, config_path)
    console.print(f"Group [bold]'{name}'[/bold] deleted.")


@main.command()
@click.option("--config", "-c", default=DEFAULT_CONFIG, help="Config file path")
def init(config):
    """Interactive wizard to set up multiple targets and groups."""
    config_path = Path(config)
    try:
        cfg = load_config(config_path)
        console.print(f"[dim]Found existing config at {config_path}[/dim]")
    except ConfigError:
        config_path.parent.mkdir(parents=True, exist_ok=True)
        cfg = QaSyncConfig()
        console.print("[bold]Welcome to qasync![/bold]")
        console.print("Let's set up your storage targets.\n")

    from qasync.setup import add_target_interactive

    # Add targets loop
    while True:
        name = console.input("\n[bold]Target name[/bold] (or 'done' to finish): ")
        name = name.strip()
        if name.lower() == "done" or not name:
            break
        add_target_interactive(cfg, name, config_path)

        # Post-add check
        if name in cfg.targets:
            console.print("\n[bold]Verifying connectivity...[/bold]")
            target = cfg.targets[name]
            cls = get_syncer_class(target.type)
            syncer = cls(target.name, target.__dict__)
            reachable, error = syncer.check()
            if reachable:
                console.print(f"  [green]Connected to {name}.[/green]")
            else:
                console.print(f"  [red]Could not reach {name}:[/red] {error}")
                console.print("  You can fix this later with 'qasync remove' + 'qasync add'.")

    if not cfg.targets:
        console.print("[yellow]No targets added. Run 'qasync init' again when ready.[/yellow]")
        return

    # Create groups
    console.print(f"\n[bold]You have {len(cfg.targets)} target(s):[/bold]")
    for name in sorted(cfg.targets):
        console.print(f"  - {name} ({cfg.targets[name].type})")

    while True:
        group_name = console.input(
            "\n[bold]Create a group?[/bold] Enter group name (or 'done' to finish): "
        )
        group_name = group_name.strip()
        if group_name.lower() == "done" or not group_name:
            break

        selected = prompt_select_targets(cfg.targets)
        cfg.groups[group_name] = selected
        save_config(cfg, config_path)
        console.print(f"  Group [bold]'{group_name}'[/bold] created: {', '.join(selected)}")

    console.print(f"\n[green bold]Setup complete![/green bold] Config saved to {config_path}")
    console.print("\nUsage:")
    console.print("  qasync upload ./test-data --group all")
    console.print("  qasync check")
    console.print("  qasync list")
