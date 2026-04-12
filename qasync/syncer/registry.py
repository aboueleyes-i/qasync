from typing import Type

from qasync.syncer.base import BaseSyncer


class RegistryError(Exception):
    pass


_registry: dict[str, Type[BaseSyncer]] = {}

# All rclone-backed types share the same syncer class
RCLONE_TYPES = {"s3", "gcs", "azureblob", "sftp", "ftp", "box", "dropbox", "drive", "local"}


def register_syncer(type_name: str, syncer_class: Type[BaseSyncer]) -> None:
    _registry[type_name] = syncer_class


def get_syncer_class(type_name: str) -> Type[BaseSyncer]:
    if type_name in _registry:
        return _registry[type_name]
    raise RegistryError(f"No syncer registered for type: {type_name}")


def _register_defaults() -> None:
    from qasync.syncer.hdfs import HdfsSyncer
    from qasync.syncer.rclone import RcloneSyncer

    for t in RCLONE_TYPES:
        register_syncer(t, RcloneSyncer)
    register_syncer("hdfs", HdfsSyncer)


_register_defaults()
