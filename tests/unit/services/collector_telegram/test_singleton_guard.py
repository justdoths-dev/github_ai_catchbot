from __future__ import annotations

from pathlib import Path

import pytest

from services.collector_telegram.exceptions import CollectorSingletonAlreadyRunningError
from services.collector_telegram.singleton_guard import CollectorSingletonGuard


def test_singleton_guard_acquires_and_releases_temp_lock_file(tmp_path: Path) -> None:
    lock_path = tmp_path / "collector.lock"
    guard = CollectorSingletonGuard(str(lock_path))

    guard.acquire()
    assert guard.is_acquired()
    assert guard.is_held()
    assert lock_path.exists()

    guard.release()
    assert not guard.is_acquired()


def test_singleton_guard_blocks_second_acquire(tmp_path: Path) -> None:
    lock_path = tmp_path / "collector.lock"

    guard_a = CollectorSingletonGuard(str(lock_path))
    guard_b = CollectorSingletonGuard(str(lock_path))

    guard_a.acquire()
    try:
        with pytest.raises(CollectorSingletonAlreadyRunningError):
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


def test_singleton_guard_repeated_acquire_is_idempotent(tmp_path: Path) -> None:
    lock_path = tmp_path / "collector.lock"
    guard = CollectorSingletonGuard(str(lock_path))

    guard.acquire()
    fd = guard._fd
    guard.acquire()

    try:
        assert guard.is_acquired()
        assert guard._fd == fd
    finally:
        guard.release()


def test_singleton_guard_repeated_release_is_idempotent(tmp_path: Path) -> None:
    lock_path = tmp_path / "collector.lock"
    guard = CollectorSingletonGuard(str(lock_path))

    guard.acquire()
    guard.release()
    guard.release()

    assert not guard.is_acquired()
