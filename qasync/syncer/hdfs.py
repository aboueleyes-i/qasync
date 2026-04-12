import time
from pathlib import Path

from qasync.syncer.base import BaseSyncer, SyncResult


class HdfsSyncer(BaseSyncer):

    def _get_filesystem(self):
        from pyarrow.fs import HadoopFileSystem  # ty: ignore[unresolved-import]

        namenode = self.config["namenode"]
        # Parse hdfs://host:port
        parts = namenode.replace("hdfs://", "").split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 8020
        return HadoopFileSystem(host=host, port=port)

    def _list_local_files(self, local_path: Path) -> list[Path]:
        return [f for f in local_path.rglob("*") if f.is_file()]

    def upload(self, local_path: Path, dry_run: bool = False, flat: bool = False) -> SyncResult:
        if not local_path.exists():
            return SyncResult(
                target_name=self.name,
                success=False,
                error=f"Local path does not exist: {local_path}",
            )

        files = self._list_local_files(local_path)
        file_count = len(files)

        if dry_run:
            return SyncResult(
                target_name=self.name,
                success=True,
                file_count=file_count,
            )

        base_path = self.config.get("base_path", "").rstrip("/")
        if not flat:
            subdir = local_path.name if local_path.is_dir() else ""
            if subdir:
                base_path = f"{base_path}/{subdir}"
        start = time.monotonic()

        try:
            fs = self._get_filesystem()

            for local_file in files:
                rel = local_file.relative_to(local_path)
                remote_path = f"{base_path}/{rel}"
                remote_dir = str(Path(remote_path).parent)
                fs.create_dir(remote_dir, recursive=True)
                fs.copy_file(str(local_file), remote_path)

            duration = time.monotonic() - start
            return SyncResult(
                target_name=self.name,
                success=True,
                file_count=file_count,
                duration_seconds=round(duration, 1),
            )
        except Exception as e:
            duration = time.monotonic() - start
            return SyncResult(
                target_name=self.name,
                success=False,
                duration_seconds=round(duration, 1),
                error=str(e),
            )

    def clean(self, remote_path: str) -> SyncResult:
        start = time.monotonic()
        try:
            fs = self._get_filesystem()
            fs.delete_dir(remote_path)
            duration = time.monotonic() - start
            return SyncResult(
                target_name=self.name,
                success=True,
                duration_seconds=round(duration, 1),
            )
        except Exception as e:
            duration = time.monotonic() - start
            return SyncResult(
                target_name=self.name,
                success=False,
                duration_seconds=round(duration, 1),
                error=str(e),
            )

    def check(self) -> tuple[bool, str]:
        try:
            fs = self._get_filesystem()
            fs.get_file_info("/")
            return True, ""
        except Exception as e:
            return False, str(e)
