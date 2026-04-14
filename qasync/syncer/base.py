from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class SyncResult:
    target_name: str
    success: bool
    file_count: int = 0
    duration_seconds: float = 0.0
    error: Optional[str] = None


class BaseSyncer(ABC):
    def __init__(self, name: str, config: dict):
        self.name = name
        self.config = config

    @abstractmethod
    def upload(
        self,
        local_path: Path,
        dry_run: bool = False,
        flat: bool = False,
        remote_path: Optional[str] = None,
    ) -> SyncResult:
        """Upload local_path contents to the remote target.

        flat=False (default): creates a subdirectory named after local_path.
        flat=True: uploads contents directly into the destination.
        remote_path: if set, overrides the target's base_path entirely.
        """

    @abstractmethod
    def clean(self, remote_path: str) -> SyncResult:
        """Delete remote_path on the target."""

    @abstractmethod
    def check(self) -> tuple[bool, str]:
        """Return (reachable, error_message). Error is empty on success."""
