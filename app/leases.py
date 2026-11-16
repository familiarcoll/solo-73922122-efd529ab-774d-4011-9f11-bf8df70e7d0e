"""Cross-instance liveness leases for in-progress file writers.

A writer (batch upload or merge output) must publish its Parquet into the
shared ``batches`` directory *before* it can register the file in SQLite.
Between those two steps the file is deliberately invisible through the API:
it has no registry row.  The orphan sweep must nevertheless never delete a
file that belongs to a live writer, no matter how long that writer pauses
(process scheduling, a long GC pause, a slow fsync, a debugger stop...).

A timeout alone cannot express "the writer is still alive".  A writer that is
SIGSTOPped for an hour is still alive and will register the file when it
resumes.  We therefore use a kernel attribute that survives arbitrary pauses
but is released automatically when the process exits: an exclusive ``flock``
on ``LEASES/<file_id>.lease``, held from before the first byte is written
until *after* the registry commit.

The sweep probes each unregistered file's lease with a non-blocking lock:

* lock held          -> a live writer owns the file; leave it alone forever;
* lock acquired      -> the owning process is gone (or it already finished);
* no lease file      -> there is no live writer for this path.

Because a writer acquires the lease before publishing the file, and releases
it only after the commit, a sweep that manages to acquire the lease knows
that no registration for that unique path can happen in the future.  It can
then re-check the registry and safely delete a genuinely abandoned file.
That ordering also closes the "sweep snapshotted the registry just before
the upload committed" race: the snapshot may be stale, but the re-check under
the lease is authoritative.

``flock`` is per open file description, so a probe in the same process as the
writer (the maintenance thread) conflicts just like a probe from another
container on the same kernel.  The shared volume is expected to provide POSIX
``flock`` semantics, as documented in the README.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import BinaryIO

try:  # POSIX (the Docker image is Linux)
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None  # type: ignore[assignment]


LEASE_SUFFIX = ".lease"


def lease_path(lease_dir: Path, key: str) -> Path:
    return lease_dir / f"{key}{LEASE_SUFFIX}"


def lease_key_for_filename(filename: str) -> str:
    """Map ``<file_id>.parquet`` / ``<file_id>.parquet.tmp`` to its key."""
    if filename.endswith(".parquet.tmp"):
        return filename[: -len(".parquet.tmp")]
    if filename.endswith(".parquet"):
        return filename[: -len(".parquet")]
    if filename.endswith(LEASE_SUFFIX):
        return filename[: -len(LEASE_SUFFIX)]
    return filename


class LeaseConflict(RuntimeError):
    """Another live process already holds the lease for this file id."""


class UploadLease:
    """Exclusive, crash-released lease held for one writer's lifetime."""

    def __init__(self, lease_dir: Path, key: str):
        self.path = lease_path(lease_dir, key)
        self._fh: BinaryIO | None = None

    def __enter__(self) -> UploadLease:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + 10.0
        while True:
            fh = open(self.path, "a+b")
            if fcntl is None:  # pragma: no cover - non-POSIX fallback
                self._fh = fh
                return self
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fh.close()
                if time.monotonic() >= deadline:
                    raise LeaseConflict(
                        f"文件租约 {self.path.name} 已被另一个活动写入者持有"
                    ) from None
                time.sleep(0.01)
                continue
            # A sweep may have unlinked the lease while it was briefly
            # unlocked (between our open and flock).  Holding a lock on an
            # unlinked inode would be invisible to later sweeps, so verify
            # the path still names our inode and retry on a fresh file.
            try:
                on_disk = os.stat(self.path)
                ours = os.fstat(fh.fileno())
            except FileNotFoundError:
                fh.close()
                continue
            if (on_disk.st_dev, on_disk.st_ino) != (ours.st_dev, ours.st_ino):
                fh.close()
                continue
            self._fh = fh
            return self

    def __exit__(self, *exc_info: object) -> None:
        if self._fh is None:
            return
        if fcntl is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        self._fh.close()
        self._fh = None
        # Best effort: a sweep that opened this lease just before the unlink
        # still holds the lock on the (now unlinked) inode and, crucially,
        # re-checks the registry before deleting anything.
        try:
            self.path.unlink(missing_ok=True)
        except OSError:  # pragma: no cover - cleanup must not fail the write
            pass


def probe_lease(lease_dir: Path, key: str) -> tuple[int | None, bool]:
    """Try to take ``key``'s lease exclusively (non-blocking).

    Returns ``(fd, active)``.  ``active`` is true when a live process holds
    the lease (the caller must not delete the file).  When ``active`` is
    false, ``fd`` is an open descriptor holding the lease guard (or ``None``
    when no lease file existed or ``flock`` is unavailable); the caller must
    ``os.close(fd)`` when done.  Holding the guard keeps any other sweep from
    racing us.
    """
    try:
        fd = os.open(lease_path(lease_dir, key), os.O_RDWR)
    except FileNotFoundError:
        return None, False
    if fcntl is None:  # pragma: no cover - non-POSIX fallback
        os.close(fd)
        return None, False
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None, True
    return fd, False
