import re
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Optional

from qasync.syncer.base import BaseSyncer, SyncResult

# Types that use bucket/container as part of the rclone path
_BUCKET_TYPES = {"s3", "gcs", "azureblob", "box", "dropbox", "drive"}

# Regex to parse rclone --stats-one-line output
# Format: "NOTICE:  1.996 MiB / 5.000 MiB, 40%, 255 KiB/s, ETA 12s"
# or:     "Transferred:   1.234 MiB / 5.678 MiB, 22%, 500 KiB/s, ETA 8s"
_STATS_RE = re.compile(r",\s*(\d+)%")


class RcloneSyncer(BaseSyncer):

    def __init__(self, name: str, config: dict, progress_callback=None):
        super().__init__(name, config)
        self.progress_callback: Optional[Callable] = progress_callback

    def _build_destination(self, subdir: str = "") -> str:
        remote = self.config.get("rclone_remote", self.name)
        base_path = self.config.get("base_path", "").strip("/")

        if self.config["type"] in _BUCKET_TYPES:
            bucket = self.config.get("bucket", "") or self.config.get("container", "")
            parts = [p for p in [bucket, base_path, subdir] if p]
            return f"{remote}:{'/'.join(parts)}"
        else:
            # sftp, ftp, local -- path only, no bucket
            parts = [p for p in [base_path, subdir] if p]
            if parts:
                return f"{remote}:/{'/'.join(parts)}"
            return f"{remote}:"

    def _build_upload_cmd(
        self, local_path: Path, dry_run: bool, subdir: str = ""
    ) -> list[str]:
        cmd = ["rclone", "copy"]
        if dry_run:
            cmd.append("--dry-run")
        # Stream stats for progress tracking
        cmd.extend(["--stats-one-line", "--stats", "1s", "--stats-log-level", "NOTICE"])
        cmd.append(str(local_path))
        cmd.append(self._build_destination(subdir))
        return cmd

    def _count_local_files(self, local_path: Path) -> int:
        return sum(1 for f in local_path.rglob("*") if f.is_file())

    def upload(self, local_path: Path, dry_run: bool = False, flat: bool = False) -> SyncResult:
        if not local_path.exists():
            return SyncResult(
                target_name=self.name,
                success=False,
                error=f"Local path does not exist: {local_path}",
            )

        file_count = self._count_local_files(local_path)

        # flat=False: create remote subdir named after source (e.g. base_path/test-data/)
        # flat=True: upload contents directly into base_path
        subdir = "" if flat else (local_path.name if local_path.is_dir() else "")

        cmd = self._build_upload_cmd(local_path, dry_run, subdir)
        start = time.monotonic()

        if self.progress_callback:
            # Stream stderr to parse progress
            proc = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            stderr_lines = []
            assert proc.stderr is not None
            for line in proc.stderr:
                stderr_lines.append(line)
                match = _STATS_RE.search(line)
                if match:
                    pct = int(match.group(1))
                    self.progress_callback(self.name, pct)
            proc.wait()
            returncode = proc.returncode
            stderr_text = "".join(stderr_lines)
        else:
            result = subprocess.run(cmd, capture_output=True, text=True)
            returncode = result.returncode
            stderr_text = result.stderr

        duration = time.monotonic() - start

        if returncode != 0:
            # Filter out stats lines, keep actual errors
            error_lines = [
                ln.strip()
                for ln in stderr_text.splitlines()
                if "ERROR" in ln or "Failed" in ln
            ]
            error = error_lines[-1] if error_lines else stderr_text.strip()
            return SyncResult(
                target_name=self.name,
                success=False,
                file_count=0,
                duration_seconds=round(duration, 1),
                error=error,
            )

        return SyncResult(
            target_name=self.name,
            success=True,
            file_count=file_count,
            duration_seconds=round(duration, 1),
        )

    def clean(self, remote_path: str) -> SyncResult:
        remote = self.config.get("rclone_remote", self.name)
        dest = f"{remote}:{remote_path.rstrip('/')}"
        start = time.monotonic()
        proc = subprocess.run(
            ["rclone", "purge", dest],
            capture_output=True,
            text=True,
        )
        duration = time.monotonic() - start
        if proc.returncode != 0:
            return SyncResult(
                target_name=self.name,
                success=False,
                duration_seconds=round(duration, 1),
                error=proc.stderr.strip(),
            )
        return SyncResult(
            target_name=self.name,
            success=True,
            duration_seconds=round(duration, 1),
        )

    def check(self) -> tuple[bool, str]:
        remote = self.config.get("rclone_remote", self.name)
        proc = subprocess.run(
            ["rclone", "lsd", f"{remote}:"],
            capture_output=True,
            text=True,
        )
        if proc.returncode == 0:
            return True, ""
        # Extract the useful part from rclone's stderr
        error = proc.stderr.strip()
        for line in error.splitlines():
            if "ERROR" in line or "Failed" in line:
                parts = line.split(" : ", 1)
                return False, parts[-1] if len(parts) > 1 else line
        return False, error.splitlines()[-1] if error else "unknown error"
