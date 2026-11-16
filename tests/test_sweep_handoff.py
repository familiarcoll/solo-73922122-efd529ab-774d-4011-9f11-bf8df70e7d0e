"""Hand-off between a file writer and the background orphan sweep.

The dangerous window is between publishing a Parquet into ``batches`` and
committing its registry row: the file exists but is not yet registered.  A
second instance sweeping orphans in that window must not delete it, even if
the upload pauses for far longer than the orphan grace period.

These tests pin down the required properties:

* a live upload is protected no matter how long it pauses;
* a file committed successfully (or pinned by an open manifest) is never
  deleted by a sweep that took its registry snapshot before the commit;
* a genuinely abandoned file from an exited writer is still reclaimed after
  the original 300 s grace period;
* a sweep that dies mid-cleanup can be continued by the next pass;
* one paused upload / deferred cleanup never blocks another instance's
  uploads, claims or downloads.

Real processes (not threads) are used where cross-process ``flock``
semantics matter.  Everything runs against a local temporary directory.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

from app.config import Settings, load_settings
from app.csv_ingest import parse_csv
from app.leases import UploadLease
from app.parquet_io import new_file_id, write_batch
from app.registry import FileLock, Registry

WORKSPACE = Path(__file__).resolve().parents[1]
HEADER = "vehicle_id,timestamp,latitude,longitude\n"
ROW = "V1,2026-01-01T00:00:00Z,30.0,120.0\n"


def make_settings(root: Path, monkeypatch, grace: float) -> Settings:
    monkeypatch.setenv("LAKE_ROOT", str(root))
    monkeypatch.setenv("LAKE_ORPHAN_GRACE_S", str(grace))
    monkeypatch.setenv("LAKE_MAINTENANCE_INTERVAL_S", "3600")
    # Keep lock-contention tests fast; production defaults are unchanged.
    monkeypatch.setenv("LAKE_MERGE_LOCK_TIMEOUT_S", "1.0")
    return load_settings()


def start(root: Path, monkeypatch, grace: float) -> tuple[Registry, Settings]:
    settings = make_settings(root, monkeypatch, grace)
    registry = Registry(settings)
    registry.start()
    return registry, settings


def age(path: Path, seconds: float) -> None:
    stale = time.time() - seconds
    os.utime(path, (stale, stale))


def publish_paused_upload(
    settings: Settings, file_id: str, batch_id: str = "slow"
):
    """Publish a batch file the way an upload does, leaving it unregistered."""
    parsed = parse_csv((HEADER + ROW).encode(), max_rows=1000)
    return write_batch(
        tmp_dir=settings.tmp_dir,
        batches_dir=settings.batches_dir,
        table=parsed.table,
        batch_id=batch_id,
        file_id=file_id,
    )


# ---------------------------------------------------------------------------
# A live upload paused inside the publish -> register window
# ---------------------------------------------------------------------------
def test_paused_upload_in_this_process_is_never_swept(
    tmp_path: Path, monkeypatch
) -> None:
    registry, settings = start(tmp_path / "data", monkeypatch, grace=0.01)
    try:
        file_id = new_file_id("b")
        with UploadLease(settings.leases_dir, file_id):
            publish_paused_upload(settings, file_id)
            path = settings.batches_dir / f"{file_id}.parquet"
            assert path.exists()
            age(path, 10_000)  # far beyond the grace period
            assert registry.sweep_orphans() == 0
            assert path.exists()

        # The upload "exited" without registering: a genuine orphan.  This
        # test uses a 0.01 s grace, so the next sweep may reclaim it.
        assert registry.sweep_orphans() == 1
        assert not path.exists()
        assert not (settings.leases_dir / f"{file_id}.lease").exists()
    finally:
        registry.stop()


def test_paused_upload_in_another_process_is_protected(
    tmp_path: Path, monkeypatch
) -> None:
    """A real second process holding the lease blocks our sweep, then exits."""
    root = tmp_path / "data"
    root.mkdir()
    env = dict(os.environ)
    env["LAKE_ROOT"] = str(root)
    env["LAKE_ORPHAN_GRACE_S"] = "0"
    env["LAKE_MAINTENANCE_INTERVAL_S"] = "3600"
    script = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {str(WORKSPACE)!r})
        from app.config import load_settings
        from app.leases import UploadLease

        settings = load_settings()
        file_id = "b_held_by_child"
        lease = UploadLease(settings.leases_dir, file_id)
        lease.__enter__()
        path = settings.batches_dir / (file_id + ".parquet")
        path.write_bytes(b"published but not registered")
        print("ready", flush=True)
        # Exit "crash-like" (no context-manager cleanup) when the parent
        # drops the stop marker; the kernel then releases the flock itself.
        stop = settings.root / "stop-child"
        while not stop.exists():
            time.sleep(0.01)
        os._exit(9)
        """
    )
    # Create the volume layout the child expects (normally done by start()).
    for directory in ("batches", "tmp", "leases"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    (root / "lake.sqlite3").touch()
    child = subprocess.Popen(
        [sys.executable, "-c", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    registry = None
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "ready"

        settings = make_settings(root, monkeypatch, grace=0.0)
        registry = Registry(settings)
        registry.start()
        path = settings.batches_dir / "b_held_by_child.parquet"
        assert path.exists()
        age(path, 10_000)
        assert registry.sweep_orphans() == 0
        assert path.exists()
        registry.stop()
        registry = None
    finally:
        (root / "stop-child").touch()
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover
            child.terminate()
            child.wait(timeout=10)
        if registry is not None:
            registry.stop()

    # The child is gone, so the kernel released its lease.  The restarted
    # instance's startup maintenance is already "the next sweep": it
    # reclaims the abandoned file with no grace extension needed.
    registry, settings = start(root, monkeypatch, grace=0.0)
    try:
        path = settings.batches_dir / "b_held_by_child.parquet"
        assert not path.exists()
        assert not (settings.leases_dir / "b_held_by_child.lease").exists()
        assert registry.stats()["live_files"] == 0
    finally:
        registry.stop()


def test_completed_upload_releases_lease_and_file_survives(
    tmp_path: Path, monkeypatch
) -> None:
    registry, settings = start(tmp_path / "data", monkeypatch, grace=0.0)
    try:
        result = registry.ingest_batch("ok", (HEADER + ROW).encode())
        file_id = result["file"].file_id
        path = settings.batches_dir / f"{file_id}.parquet"
        assert path.exists()
        age(path, 10_000)
        assert registry.sweep_orphans() == 0
        assert path.exists()
        assert not (settings.leases_dir / f"{file_id}.lease").exists()
        assert registry.list_live()["file_count"] == 1
    finally:
        registry.stop()


# ---------------------------------------------------------------------------
# Sweep snapshot taken before a registration commits
# ---------------------------------------------------------------------------
def test_registration_after_sweep_snapshot_is_not_deleted(
    tmp_path: Path, monkeypatch
) -> None:
    """The exact reported race: snapshot first, registration second.

    An unregistered file is placed on disk, then the sweep starts and takes
    its registry snapshot.  While the scan is running, another writer
    registers that same path.  The snapshot does not know about it, but the
    lease-guarded re-read does, so the file must survive.
    """
    registry, settings = start(tmp_path / "data", monkeypatch, grace=0.0)
    try:
        file_id = new_file_id("b")
        written = publish_paused_upload(settings, file_id, batch_id="raced")
        path = settings.batches_dir / f"{file_id}.parquet"
        age(path, 10_000)

        from app import db

        real_check = registry._sweep_candidate_is_abandoned
        registered = {"done": False}

        def check_with_racing_registration(candidate: Path) -> bool:
            if not registered["done"] and candidate == path:
                registered["done"] = True
                # Emulate another instance committing its registry row after
                # this sweep took its snapshot; its lease is already released.
                with registry.connection() as conn, db.write_tx(conn):
                    conn.execute(
                        """
                        INSERT INTO files
                            (id, kind, rel_path, rows, bytes, sha256, csv_sha256,
                             source_batch_id, merge_id, created_at, live)
                        VALUES (?, 'batch', ?, ?, ?, ?, ?, ?, NULL, ?, 1)
                        """,
                        (
                            file_id,
                            written.rel_path,
                            written.row_count,
                            written.byte_size,
                            written.sha256,
                            "x",
                            "raced",
                            "2026-01-01T00:00:00Z",
                        ),
                    )
            return real_check(candidate)

        monkeypatch.setattr(
            registry,
            "_sweep_candidate_is_abandoned",
            check_with_racing_registration,
        )
        assert registry.sweep_orphans() == 0
        assert registered["done"] is True
        assert path.exists()
        assert registry.stats()["live_files"] == 1
        # And the file stays downloadable through a manifest.
        manifest = registry.claim()
        handle, size, _, _ = registry.open_for_download(
            manifest["manifest_id"], file_id
        )
        with handle:
            assert len(handle.read()) == size
    finally:
        registry.stop()


def test_manifest_pin_blocks_recycling_under_race(
    tmp_path: Path, monkeypatch
) -> None:
    registry, settings = start(tmp_path / "data", monkeypatch, grace=0.0)
    try:
        first = registry.ingest_batch("a", (HEADER + ROW).encode())["file"]
        second = registry.ingest_batch(
            "b", (HEADER + ROW.replace("V1", "V2")).encode()
        )["file"]
        manifest = registry.claim()
        merged = registry.merge()
        # The merged output is pinned by the open manifest, so neither input
        # may be recycled while it is open.
        assert registry.recycle_retired() == 0
        for item in (first, second):
            path = settings.batches_dir / f"{item.file_id}.parquet"
            assert path.exists()
            assert registry._sweep_candidate_is_abandoned(path) is False
        assert registry.sweep_orphans() == 0
        assert (settings.batches_dir / f"{first.file_id}.parquet").exists()
        # Releasing triggers the recycle inline; both retired inputs go away.
        registry.release(manifest["manifest_id"])
        assert registry.recycle_retired() == 0
        for item in (first, second):
            assert not (settings.batches_dir / f"{item.file_id}.parquet").exists()
        assert merged["output_file_id"] in {
            item.file_id for item in registry.list_live()["files"]
        }
    finally:
        registry.stop()


# ---------------------------------------------------------------------------
# Genuinely abandoned files still reclaim after the original 300 s grace
# ---------------------------------------------------------------------------
def test_abandoned_file_still_respects_default_grace(
    tmp_path: Path, monkeypatch
) -> None:
    registry, settings = start(tmp_path / "data", monkeypatch, grace=300.0)
    try:
        orphan = settings.batches_dir / "m_crashed.parquet"
        orphan.write_bytes(b"half a merge output")
        assert registry.sweep_orphans() == 0  # younger than 300 s
        assert orphan.exists()
        age(orphan, 299)
        assert registry.sweep_orphans() == 0
        assert orphan.exists()
        age(orphan, 301)
        assert registry.sweep_orphans() == 1
        assert not orphan.exists()
    finally:
        registry.stop()


def test_sweep_can_resume_after_interruption(tmp_path: Path, monkeypatch) -> None:
    """If cleanup dies mid-way, the next sweep finishes the same work."""
    registry, settings = start(tmp_path / "data", monkeypatch, grace=0.0)
    try:
        file_id = new_file_id("b")
        publish_paused_upload(settings, file_id, batch_id="interrupted")
        path = settings.batches_dir / f"{file_id}.parquet"
        # A crashed writer leaves its (now unlocked) lease file behind.
        leftover_lease = settings.leases_dir / f"{file_id}.lease"
        leftover_lease.write_bytes(b"")
        age(path, 10_000)
        age(leftover_lease, 10_000)

        # Emulate dying after the data file was unlinked but before the lease
        # was cleaned up: the next production sweep is idempotent and tidies
        # up the remaining lease (that tidy-up counts as one removal).
        path.unlink()
        assert registry.sweep_orphans() == 1
        assert not leftover_lease.exists()

        # And a fresh abandoned file is still reclaimable afterwards.
        other_id = new_file_id("b")
        publish_paused_upload(settings, other_id, batch_id="after-interrupt")
        other = settings.batches_dir / f"{other_id}.parquet"
        age(other, 10_000)
        assert registry.sweep_orphans() == 1
        assert not other.exists()
    finally:
        registry.stop()


# ---------------------------------------------------------------------------
# One stalled upload / cleanup must not block another instance
# ---------------------------------------------------------------------------
def test_paused_upload_does_not_block_other_instance(
    tmp_path: Path, monkeypatch
) -> None:
    registry, settings = start(tmp_path / "data", monkeypatch, grace=0.0)
    peer = Registry(settings)
    peer.start()
    try:
        paused_id = new_file_id("b")
        with UploadLease(settings.leases_dir, paused_id):
            publish_paused_upload(settings, paused_id, batch_id="paused")
            age(settings.batches_dir / f"{paused_id}.parquet", 10_000)

            # Meanwhile the other instance completes full batches, claims
            # and downloads, and its sweep leaves the paused upload alone.
            result = peer.ingest_batch(
                "peer-1", (HEADER + ROW.replace("V1", "V3")).encode()
            )
            assert result["file"].rows == 1
            manifest = peer.claim("peer-job")
            assert manifest["file_count"] == 1
            handle, size, _, _ = peer.open_for_download(
                manifest["manifest_id"], result["file"].file_id
            )
            with handle:
                assert len(handle.read()) == size
            assert peer.sweep_orphans() == 0
            assert (
                settings.batches_dir / f"{paused_id}.parquet"
            ).exists()
    finally:
        peer.stop()
        registry.stop()


def test_cleanup_waiting_on_merge_does_not_block_peer(tmp_path: Path, monkeypatch) -> None:
    """A deferred per-file recycle only postpones cleanup, never traffic."""
    registry, settings = start(tmp_path / "data", monkeypatch, grace=0.0)
    peer = Registry(settings)
    peer.start()
    try:
        old = peer.ingest_batch("old-1", (HEADER + ROW).encode())["file"]
        peer.ingest_batch("old-2", (HEADER + ROW.replace("V1", "V2")).encode())
        retired_path = settings.batches_dir / f"{old.file_id}.parquet"
        with FileLock(settings.lock_path, 0.1):
            # The merge lock is "held by a paused merge": the merge itself
            # fails to start, but the sweep is not blocked by it at all.
            with __import__("pytest").raises(Exception):
                peer.merge()
            assert retired_path.exists()
            # But the peer keeps uploading, claiming and downloading.
            extra = peer.ingest_batch(
                "peer-during-lock", (HEADER + ROW.replace("V1", "V5")).encode()
            )
            manifest = peer.claim("during-lock")
            for item in manifest["files"]:
                handle, size, _, _ = peer.open_for_download(
                    manifest["manifest_id"], item["file_id"]
                )
                with handle:
                    assert len(handle.read()) == size
            assert extra["file"].file_id in {
                item["file_id"] for item in manifest["files"]
            }
            # A full maintenance pass (recycle + sweep, incl. its own sweep
            # lock) completes while the merge lock is held.
            assert registry.run_maintenance().get("skipped", False) is False
            peer.release(manifest["manifest_id"])

        # Once the lock is free, a merge retires the inputs and the deferred
        # cleanup completes: nothing is stranded.
        merged = peer.merge()
        assert not retired_path.exists()
        live = {item.file_id for item in peer.list_live()["files"]}
        assert merged["output_file_id"] in live
        assert old.file_id not in live
    finally:
        peer.stop()
        registry.stop()
