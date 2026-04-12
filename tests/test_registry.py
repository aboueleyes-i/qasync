import pytest

from qasync.syncer.base import BaseSyncer
from qasync.syncer.registry import RegistryError, get_syncer_class, register_syncer


def test_get_registered_syncer():
    class FakeSyncer(BaseSyncer):
        def upload(self, local_path, dry_run=False):
            pass

        def clean(self, remote_path):
            pass

        def check(self):
            return True

    register_syncer("fake", FakeSyncer)
    assert get_syncer_class("fake") is FakeSyncer


def test_get_unknown_syncer():
    with pytest.raises(RegistryError, match="No syncer registered"):
        get_syncer_class("unknown_type_xyz")


def test_rclone_syncer_registered():
    cls = get_syncer_class("s3")
    assert cls.__name__ == "RcloneSyncer"


def test_hdfs_syncer_registered():
    cls = get_syncer_class("hdfs")
    assert cls.__name__ == "HdfsSyncer"
