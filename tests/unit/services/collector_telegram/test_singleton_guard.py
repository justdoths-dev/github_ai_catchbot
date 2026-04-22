from __future__ import annotations

from pathlib import Path

import pytest

from services.collector_telegram.exceptions import SingletonViolationError
from services.collector_telegram.singleton_guard import CollectorSingletonGuard


def test_singleton_guard_blocks_second_acquire(tmp_path: Path) -> None:
    lock_path = tmp_path / "collector.lock"

    guard_a = CollectorSingletonGuard(str(lock_path))
    guard_b = CollectorSingletonGuard(str(lock_path))

    guard_a.acquire()
    try:
        with pytest.raises(SingletonViolationError):
            guard_b.acquire()
    finally:
        guard_a.release()


def test_singleton_guard_release_allows_reacquire(tmp_path: Path) -> None:
    lock_path = tmp_path / "collector.lock"

    guard_a = CollectorSingletonGuard(str(lock_path))
    guard_b = CollectorSingletonGuard(str(lock_path))

    guard_a.acquire()
    guard_a.release()
    guard_b.acquire()
    guard_b.release()