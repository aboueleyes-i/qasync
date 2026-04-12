

from qasync.runner import run_sync
from qasync.syncer.base import BaseSyncer, SyncResult


class FakeSyncer(BaseSyncer):
    def __init__(self, name, config, should_fail=False):
        super().__init__(name, config)
        self.should_fail = should_fail
        self.uploaded = False

    def upload(self, local_path, dry_run=False, flat=False):
        self.uploaded = True
        if self.should_fail:
            return SyncResult(target_name=self.name, success=False, error="mock error")
        return SyncResult(
            target_name=self.name, success=True, file_count=5, duration_seconds=1.0
        )

    def clean(self, remote_path):
        return SyncResult(target_name=self.name, success=True)

    def check(self):
        if self.should_fail:
            return False, "mock check error"
        return True, ""


def test_run_sync_all_succeed(tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    syncers = [FakeSyncer("s3", {}), FakeSyncer("gcs", {})]
    results = run_sync(syncers, test_dir, max_parallel=2)
    assert len(results) == 2
    assert all(r.success for r in results)


def test_run_sync_partial_failure(tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    syncers = [FakeSyncer("s3", {}), FakeSyncer("azure", {}, should_fail=True)]
    results = run_sync(syncers, test_dir, max_parallel=2)
    assert len(results) == 2
    s3_result = next(r for r in results if r.target_name == "s3")
    azure_result = next(r for r in results if r.target_name == "azure")
    assert s3_result.success is True
    assert azure_result.success is False


def test_run_sync_respects_parallel_cap(tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    syncers = [FakeSyncer(f"target-{i}", {}) for i in range(5)]
    results = run_sync(syncers, test_dir, max_parallel=2)
    assert len(results) == 5
    assert all(r.success for r in results)


def test_run_sync_dry_run(tmp_path):
    test_dir = tmp_path / "data"
    test_dir.mkdir()
    syncers = [FakeSyncer("s3", {})]
    results = run_sync(syncers, test_dir, max_parallel=1, dry_run=True)
    assert len(results) == 1
