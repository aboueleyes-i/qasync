from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)

from qasync.syncer.base import BaseSyncer, SyncResult
from qasync.syncer.rclone import RcloneSyncer


def _make_progress():
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
    )


def run_sync(
    syncers: list[BaseSyncer],
    local_path: Path,
    max_parallel: int = 3,
    dry_run: bool = False,
    flat: bool = False,
    remote_paths: Optional[dict[str, str]] = None,
) -> list[SyncResult]:
    results: list[SyncResult] = []
    remote_paths = remote_paths or {}

    with _make_progress() as progress:
        # Create a progress task per syncer
        task_ids = {}
        for syncer in syncers:
            tid = progress.add_task(f"  {syncer.name}", total=100)
            task_ids[syncer.name] = tid

        def make_callback(name):
            def cb(target_name, pct):
                progress.update(task_ids[name], completed=pct)
            return cb

        # Wire up progress callbacks for rclone syncers
        for syncer in syncers:
            if isinstance(syncer, RcloneSyncer):
                syncer.progress_callback = make_callback(syncer.name)

        with ThreadPoolExecutor(max_workers=max_parallel) as pool:
            futures = {
                pool.submit(
                    syncer.upload, local_path, dry_run, flat,
                    remote_paths.get(syncer.name),
                ): syncer
                for syncer in syncers
            }
            for future in as_completed(futures):
                syncer = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = SyncResult(
                        target_name=syncer.name,
                        success=False,
                        error=str(e),
                    )
                results.append(result)
                # Mark complete
                tid = task_ids[syncer.name]
                label = "[green]done[/green]" if result.success else "[red]failed[/red]"
                progress.update(
                    tid, completed=100, description=f"  {syncer.name} {label}"
                )

    return results


def run_check(
    syncers: list[BaseSyncer],
    max_parallel: int = 3,
) -> dict[str, tuple[bool, str]]:
    results: dict[str, tuple[bool, str]] = {}

    with _make_progress() as progress:
        task = progress.add_task("Checking", total=len(syncers))

        with ThreadPoolExecutor(max_workers=max_parallel) as pool:
            futures = {
                pool.submit(syncer.check): syncer
                for syncer in syncers
            }
            for future in as_completed(futures):
                syncer = futures[future]
                try:
                    results[syncer.name] = future.result()
                except Exception as e:
                    results[syncer.name] = (False, str(e))
                progress.update(task, advance=1, description=f"Checking  {syncer.name}")

    return results


def run_clean(
    syncers: list[BaseSyncer],
    remote_path: str,
    max_parallel: int = 3,
) -> list[SyncResult]:
    results: list[SyncResult] = []

    with _make_progress() as progress:
        task = progress.add_task("Cleaning", total=len(syncers))

        with ThreadPoolExecutor(max_workers=max_parallel) as pool:
            futures = {
                pool.submit(syncer.clean, remote_path): syncer
                for syncer in syncers
            }
            for future in as_completed(futures):
                syncer = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = SyncResult(
                        target_name=syncer.name,
                        success=False,
                        error=str(e),
                    )
                results.append(result)
                progress.update(task, advance=1, description=f"Cleaning  {syncer.name}")

    return results
