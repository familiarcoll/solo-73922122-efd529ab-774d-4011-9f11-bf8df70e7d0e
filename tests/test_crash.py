"""Crash-injection tests.

A merge may be killed after its Parquet is published but before (or during)
the registry commit.  Either way, the new file list must be exactly the
pre-crash one, no half-merged result may be exposed, and a retry must
produce a correct list.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from app.config import Settings
from app.registry import Registry

WORKSPACE = Path(__file__).resolve().parents[1]

CRASH_SCRIPT = textwrap.dedent(
    """
    import json, os, sys
    from pathlib import Path

    sys.path.insert(0, {workspace!r})
    from app.config import load_settings
    from app import db
    from app.registry import Registry

    stage = sys.argv[1]
    registry = Registry(load_settings())
    registry.start()

    if stage == "before_commit":
        def crash(conn, **kwargs):
            os._exit(9)
        registry._commit_merge = crash
    elif stage == "mid_commit":
        def crash(conn, **kwargs):
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO files (id, kind, rel_path, rows, bytes, sha256, "
                "source_batch_id, merge_id, created_at, live) "
                "VALUES ('half', 'merged', 'batches/half.parquet', 1, 1, 'x', "
                "NULL, 'mghalf', '2026-01-01T00:00:00Z', 1)");
            os._exit(9)
        registry._commit_merge = crash
    else:
        raise SystemExit(2)

    registry.merge()
    raise SystemExit(0)
    """
)


def _run_crash(root: Path, stage: str) -> int:
    script = CRASH_SCRIPT.format(workspace=str(WORKSPACE))
    env = dict(os.environ)
    env["LAKE_ROOT"] = str(root)
    env["LAKE_MAINTENANCE_INTERVAL_S"] = "3600"
    completed = subprocess.run(
        [sys.executable, "-c", script, stage],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return completed.returncode


def _settings(root: Path) -> Settings:
    os.environ["LAKE_ROOT"] = str(root)
    os.environ["LAKE_MAINTENANCE_INTERVAL_S"] = "3600"
    from app.config import load_settings

    return load_settings()


CSV_ROWS = "".join(
    f"V{index % 3},{2026}-01-01T00:00:{index:02d}Z,{30 + index / 100}.0,120.0\n"
    for index in range(10)
)


def _seed(root: Path) -> list[str]:
    settings = _settings(root)
    registry = Registry(settings)
    registry.start()
    try:
        ids = []
        for index in range(3):
            payload = (
                "vehicle_id,timestamp,latitude,longitude\n"
                + "".join(
                    f"V{index},{2026}-01-0{index + 1}T00:00:{row:02d}Z,"
                    f"{30.0 + row / 100},{120.0 + index}\n"
                    for row in range(6)
                )
            ).encode()
            ids.append(
                registry.ingest_batch(f"seed-{index}", payload)["file"].file_id
            )
        return ids
    finally:
        registry.stop()


@pytest.mark.parametrize("stage", ["before_commit", "mid_commit"])
def test_crash_during_merge_keeps_previous_list_and_allows_retry(
    tmp_path: Path, stage: str
) -> None:
    root = tmp_path / "data"
    root.mkdir()
    seeded = _seed(root)

    code = _run_crash(root, stage)
    assert code == 9, "crash injection should terminate the merge process"

    settings = _settings(root)
    registry = Registry(settings)
    registry.start()
    try:
        live = registry.list_live()
        assert [item.file_id for item in live["files"]] == seeded
        assert live["row_count"] == 18
        assert "half" not in {item.file_id for item in live["files"]}

        # The half-written merge output is on disk but not registered, and
        # is therefore invisible until the orphan sweep reclaims it.
        orphan_files = [p for p in settings.batches_dir.iterdir() if p.is_file()]
        assert len(orphan_files) == len(seeded) + 1
        abandoned = [p for p in orphan_files if p.stem not in seeded]
        assert len(abandoned) == 1
        for path in orphan_files:
            if path.stem in seeded:
                # Every live file remains fully readable.
                table = pq.read_table(path)
                assert table.num_rows == 6

        result = registry.merge()
        assert result["row_count"] == 18
        assert result["input_count"] == 3
        after = registry.list_live()
        assert [item.file_id for item in after["files"]] == [
            result["output_file_id"]
        ]
        assert after["row_count"] == 18
        out_path = settings.batches_dir / f"{result['output_file_id']}.parquet"
        assert pq.read_table(out_path).num_rows == 18

        # The abandoned output becomes an orphan candidate and is removed.
        assert abandoned[0].exists()
        os.utime(abandoned[0], (0, 0))
        registry.sweep_orphans()
        remaining = {p.stem for p in settings.batches_dir.iterdir()}
        assert remaining == {result["output_file_id"]}
    finally:
        registry.stop()


def test_mid_commit_crash_does_not_leave_half_rows(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    seeded = _seed(root)
    env = dict(os.environ)
    env["LAKE_ROOT"] = str(root)
    env["LAKE_MAINTENANCE_INTERVAL_S"] = "3600"
    assert _run_crash(root, "mid_commit") == 9

    # A fresh connection must see neither the output row nor the retirements.
    settings = _settings(root)
    from app import db

    conn = db.connect(settings.db_path, settings.db_busy_timeout_ms)
    try:
        ids = [row["id"] for row in conn.execute("SELECT id FROM files ORDER BY seq")]
        live = [
            row["id"]
            for row in conn.execute("SELECT id FROM files WHERE live = 1 ORDER BY seq")
        ]
        assert ids == seeded
        assert live == seeded
        assert conn.execute("SELECT COUNT(*) FROM merges").fetchone()[0] == 0
    finally:
        conn.close()


def test_registration_failure_discards_written_batch(tmp_path: Path, monkeypatch) -> None:
    """If the DB insert fails, the already-written Parquet is not left behind."""
    root = tmp_path / "data"
    root.mkdir()
    settings = _settings(root)
    registry = Registry(settings)
    registry.start()
    try:
        from app import db as db_module

        class Boom(Exception):
            pass

        def failing_write_tx(conn):
            raise Boom()

        monkeypatch.setattr(db_module, "write_tx", failing_write_tx)
        with pytest.raises(Boom):
            registry.ingest_batch(
                "will-fail",
                b"vehicle_id,timestamp,latitude,longitude\n"
                b"V1,2026-01-01T00:00:00Z,1,2\n",
            )
        assert list(settings.batches_dir.iterdir()) == []
        assert list(settings.tmp_dir.iterdir()) == []
        assert registry.stats()["total_files"] == 0
    finally:
        registry.stop()


def test_two_instances_can_bootstrap_the_same_volume(tmp_path: Path) -> None:
    root = tmp_path / "data"
    root.mkdir()
    from concurrent.futures import ThreadPoolExecutor

    settings = _settings(root)
    failures: list[BaseException] = []

    def boot() -> None:
        registry = Registry(settings)
        try:
            registry.start()
            registry.ingest_batch(
                f"boot-{id(registry)}",
                b"vehicle_id,timestamp,latitude,longitude\n"
                b"V1,2026-01-01T00:00:00Z,1,2\n",
            )
        except BaseException as exc:  # pragma: no cover
            failures.append(exc)
        finally:
            registry.stop()

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: boot(), range(4)))
    assert not failures

    final = Registry(settings)
    final.start()
    try:
        assert final.list_live()["file_count"] == 4
    finally:
        final.stop()
