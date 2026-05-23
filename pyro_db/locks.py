"""
pyro_db.locks
=============
File-based locking primitives for cross-process and cross-thread safety.

Strategy
--------
We use two complementary mechanisms:

1. **``threading.RLock``** — protects in-process concurrent access within a
   single interpreter (multiple threads sharing one ``Database`` instance).
2. **Advisory file lock via ``fcntl.flock`` (POSIX) or ``msvcrt.locking``
   (Windows)** — protects cross-process access when two separate Python
   processes open the same database directory simultaneously.

The :class:`CollectionLock` context manager acquires both layers before
yielding and releases them in reverse order on exit.

Timeout
-------
If the file lock cannot be acquired within ``timeout`` seconds a
:class:`~pyro_db.exceptions.LockTimeoutError` is raised.  The default is
10 seconds.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from pyro_db.exceptions import LockTimeoutError

# ---------------------------------------------------------------------------
# Platform-specific lock primitives
# ---------------------------------------------------------------------------

if sys.platform == "win32":
    import msvcrt

    def _lock_file(fh, exclusive: bool, timeout: float) -> None:  # type: ignore[misc]
        """Acquire an advisory lock on Windows using ``msvcrt``."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    def _unlock_file(fh) -> None:  # type: ignore[misc]
        """Release an advisory lock on Windows."""
        try:
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass

else:
    import fcntl

    def _lock_file(fh, exclusive: bool, timeout: float) -> None:
        """Acquire an advisory lock on POSIX using ``fcntl.flock``.

        Parameters
        ----------
        fh : IO
            Open file handle whose file descriptor will be locked.
        exclusive : bool
            ``True`` for an exclusive (write) lock; ``False`` for a shared
            (read) lock.
        timeout : float
            Seconds to wait before raising ``OSError``.
        """
        flag = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        deadline = time.monotonic() + timeout
        while True:
            try:
                fcntl.flock(fh.fileno(), flag | fcntl.LOCK_NB)
                return
            except OSError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.01)

    def _unlock_file(fh) -> None:
        """Release a ``fcntl``-based lock."""
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# CollectionLock
# ---------------------------------------------------------------------------

class CollectionLock:
    """A two-layer lock (threading + file) for a single database collection.

    Parameters
    ----------
    lock_path : Path
        Path to the ``.lock`` sentinel file for this collection.
    timeout : float
        Maximum seconds to wait for the file lock.

    Example
    -------
    ::

        lock = CollectionLock(Path("mydb/users.lock"))
        with lock.write():
            # exclusive access
            ...
        with lock.read():
            # shared access
            ...
    """

    def __init__(self, lock_path: Path, timeout: float = 10.0):
        self._lock_path = Path(lock_path)
        self._timeout = timeout
        self._rlock = threading.RLock()

    # ------------------------------------------------------------------
    # Context managers
    # ------------------------------------------------------------------

    @contextmanager
    def write(self) -> Generator[None, None, None]:
        """Acquire an **exclusive** (write) lock.

        Blocks until the lock is available or the timeout is reached.

        Yields
        ------
        None

        Raises
        ------
        LockTimeoutError
            If the file lock cannot be acquired within the timeout.
        """
        with self._rlock:
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._lock_path.touch(exist_ok=True)
            with open(self._lock_path, "a") as fh:
                try:
                    _lock_file(fh, exclusive=True, timeout=self._timeout)
                except OSError as exc:
                    raise LockTimeoutError(
                        str(self._lock_path), self._timeout
                    ) from exc
                try:
                    yield
                finally:
                    _unlock_file(fh)

    @contextmanager
    def read(self) -> Generator[None, None, None]:
        """Acquire a **shared** (read) lock.

        Multiple concurrent readers are allowed; a writer will block until
        all readers release.

        Yields
        ------
        None

        Raises
        ------
        LockTimeoutError
            If the file lock cannot be acquired within the timeout.
        """
        with self._rlock:
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._lock_path.touch(exist_ok=True)
            with open(self._lock_path, "r") as fh:
                try:
                    _lock_file(fh, exclusive=False, timeout=self._timeout)
                except OSError as exc:
                    raise LockTimeoutError(
                        str(self._lock_path), self._timeout
                    ) from exc
                try:
                    yield
                finally:
                    _unlock_file(fh)


# ---------------------------------------------------------------------------
# DatabaseLock — database-level (broader) exclusive lock
# ---------------------------------------------------------------------------

class DatabaseLock:
    """A coarser exclusive lock that covers the entire database directory.

    Used during compaction and schema-wide operations where we need to
    prevent any collection from being modified.

    Parameters
    ----------
    db_path : Path
        Root directory of the database.
    timeout : float
        Maximum seconds to wait for the lock.
    """

    def __init__(self, db_path: Path, timeout: float = 30.0):
        self._lock_path = Path(db_path) / ".db.lock"
        self._timeout = timeout
        self._rlock = threading.RLock()

    @contextmanager
    def acquire(self) -> Generator[None, None, None]:
        """Acquire the database-level exclusive lock.

        Yields
        ------
        None

        Raises
        ------
        LockTimeoutError
            If the lock cannot be acquired within the timeout.
        """
        with self._rlock:
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            self._lock_path.touch(exist_ok=True)
            with open(self._lock_path, "a") as fh:
                try:
                    _lock_file(fh, exclusive=True, timeout=self._timeout)
                except OSError as exc:
                    raise LockTimeoutError(
                        str(self._lock_path), self._timeout
                    ) from exc
                try:
                    yield
                finally:
                    _unlock_file(fh)
