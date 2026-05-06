from __future__ import annotations

import fcntl
import os
from dataclasses import dataclass
from pathlib import Path

from .exceptions import CollectorSingletonAlreadyRunningError


@dataclass(slots=True)
class CollectorSingletonGuard:
    """Process-level singleton guard for live collector.

    This is intentionally Linux/VPS-oriented because prod runtime is already fixed to
    a single Ubuntu-class VPS. `flock` releases automatically when the process exits,
    which makes it suitable for restart recovery without stale manual cleanup.
    """

    lock_path: str
    _fd: int | None = None

    def acquire(self) -> None:
        if self._fd is not None:
            return

        path = Path(self.lock_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise CollectorSingletonAlreadyRunningError(
                f"collector singleton already held: {self.lock_path}"
            ) from exc

        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None

    def is_acquired(self) -> bool:
        return self._fd is not None

    def is_held(self) -> bool:
        return self.is_acquired()

    def __enter__(self) -> "CollectorSingletonGuard":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
