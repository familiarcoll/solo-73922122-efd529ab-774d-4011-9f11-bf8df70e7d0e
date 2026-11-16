from __future__ import annotations

import csv
import io
import os
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from app.config import Settings, load_settings
from app.registry import Registry


def make_csv(rows: list[tuple[str, str, float, float]], header: bool = True) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    if header:
        writer.writerow(["vehicle_id", "timestamp", "latitude", "longitude"])
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def rows_for(batch_index: int, count: int, prefix: str = "V") -> list[tuple[str, str, float, float]]:
    return [
        (
            f"{prefix}{batch_index:03d}",
            f"2026-01-{1 + batch_index % 28:02d}T00:{index // 60:02d}:{index % 60:02d}.{batch_index:06d}Z",
            30.0 + index / 1000,
            120.0 + index / 1000,
        )
        for index in range(count)
    ]


@dataclass
class Lake:
    registry: Registry
    settings: Settings

    def upload(self, batch_id: str, rows) -> dict:
        return self.registry.ingest_batch(batch_id, make_csv(rows))


@pytest.fixture()
def lake(tmp_path: Path, monkeypatch) -> Lake:
    root = tmp_path / "data"
    monkeypatch.setenv("LAKE_ROOT", str(root))
    monkeypatch.setenv("LAKE_ORPHAN_GRACE_S", "0.2")
    monkeypatch.setenv("LAKE_MAINTENANCE_INTERVAL_S", "3600")
    settings = load_settings()
    registry = Registry(settings)
    registry.start()
    yield Lake(registry=registry, settings=settings)
    registry.stop()


def read_rows(path: Path) -> list[tuple[str, str, float, float]]:
    table = pq.read_table(path)
    return list(
        zip(
            table.column("vehicle_id").to_pylist(),
            [_timestamp_text(value) for value in table.column("timestamp").to_pylist()],
            table.column("latitude").to_pylist(),
            table.column("longitude").to_pylist(),
            strict=True,
        )
    )


def manifest_rows(lake: Lake, manifest: dict) -> list[tuple[str, str, float, float]]:
    rows: list[tuple[str, str, float, float]] = []
    for item in manifest["files"]:
        handle, _size, _sha, _name = lake.registry.open_for_download(
            manifest["manifest_id"], item["file_id"]
        )
        with handle:
            data = handle.read()
        # Read via pyarrow from the downloaded bytes.
        import pyarrow as pa

        rows.extend(
            read_rows_from_bytes(pa.BufferReader(data))
        )
    return rows


def read_rows_from_bytes(reader) -> list[tuple[str, str, float, float]]:
    table = pq.read_table(reader)
    return list(
        zip(
            table.column("vehicle_id").to_pylist(),
            [_timestamp_text(value) for value in table.column("timestamp").to_pylist()],
            table.column("latitude").to_pylist(),
            table.column("longitude").to_pylist(),
            strict=True,
        )
    )


def live_file_path(lake: Lake, file_id: str) -> Path:
    return lake.settings.batches_dir / f"{file_id}.parquet"


# --------------------------------------------------------------------- tests
def test_duplicate_rows_are_preserved(lake: Lake) -> None:
    duplicate = ("V1", "2026-01-01T00:00:00Z", 30.0, 120.0)
    result = lake.upload("b1", [duplicate, duplicate, duplicate])
    assert result["file"].rows == 3
    live = lake.registry.list_live()
    assert live["row_count"] == 3
    assert read_rows(live_file_path(lake, result["file"].file_id)) == [norm([duplicate])[0]] * 3


def test_duplicate_rows_survive_merges_with_multiplicity(lake: Lake) -> None:
    repeated = ("V9", "2026-02-01T00:00:00Z", 40.0, 130.0)
    lake.upload("d1", [repeated, repeated])
    lake.upload("d2", [repeated, ("V9", "2026-02-01T00:00:01Z", 40.0, 130.0)])
    first = lake.registry.merge()
    assert first["row_count"] == 4
    rows = read_rows(live_file_path(lake, first["output_file_id"]))
    assert rows.count(norm([repeated])[0]) == 3
    second = lake.registry.merge()
    assert second["row_count"] == 4
    merged = read_rows(live_file_path(lake, second["output_file_id"]))
    assert merged.count(norm([repeated])[0]) == 3
    assert lake.registry.list_live()["row_count"] == 4


def test_merge_only_replaces_its_snapshot(lake: Lake) -> None:
    first = lake.upload("b1", rows_for(1, 5))["file"]
    second = lake.upload("b2", rows_for(2, 4))["file"]

    merge = lake.registry.merge()
    assert merge["input_count"] == 2
    assert set(merge["input_file_ids"]) == {first.file_id, second.file_id}
    assert merge["row_count"] == 9

    # A batch uploaded *after* the merge began but before it returned is not
    # part of the snapshot.  Emulate that ordering by uploading immediately
    # after the commit: the merge must not have swallowed it.
    late = lake.upload("b3", rows_for(3, 6))["file"]
    live = lake.registry.list_live()
    live_ids = {item.file_id for item in live["files"]}
    assert late.file_id in live_ids
    assert merge["output_file_id"] in live_ids
    assert first.file_id not in live_ids and second.file_id not in live_ids
    assert live["row_count"] == 9 + 6
    assert live["file_count"] == 2

    merged_rows = read_rows(live_file_path(lake, merge["output_file_id"]))
    assert merged_rows == norm(rows_for(1, 5) + rows_for(2, 4))


def test_open_manifest_survives_merge_release_and_restart(lake: Lake) -> None:
    a = lake.upload("b1", rows_for(1, 3))["file"]
    b = lake.upload("b2", rows_for(2, 2))["file"]
    expected = norm(rows_for(1, 3) + rows_for(2, 2))
    manifest = lake.registry.claim("analysis-job-1")
    pinned = [item["file_id"] for item in manifest["files"]]
    assert pinned == [a.file_id, b.file_id]
    assert manifest_rows(lake, manifest) == expected

    # Merge retires the pinned inputs, but the open manifest keeps them.
    merge = lake.registry.merge()
    lake.registry.recycle_retired()
    payload = lake.registry.get_manifest(manifest["manifest_id"])
    assert [item["file_id"] for item in payload["files"]] == pinned
    for item in payload["files"]:
        handle, size, sha256, _ = lake.registry.open_for_download(
            manifest["manifest_id"], item["file_id"]
        )
        with handle:
            data = handle.read()
        assert len(data) == size == item["bytes"]
        assert _digest(data) == sha256
    assert manifest_rows(lake, payload) == expected

    # A new manifest sees only the merged file, with the same rows.
    fresh = lake.registry.claim("analysis-job-2")
    assert [item["file_id"] for item in fresh["files"]] == [
        merge["output_file_id"]
    ]
    assert manifest_rows(lake, fresh) == expected

    # Simulate a full restart on the same volume.
    lake.registry.stop()
    restarted = Registry(lake.settings)
    restarted.start()
    try:
        again = restarted.get_manifest(manifest["manifest_id"])
        assert [item["file_id"] for item in again["files"]] == pinned
        restarted.recycle_retired()
        restarted.sweep_orphans()
        for item in again["files"]:
            handle, _, _, _ = restarted.open_for_download(
                manifest["manifest_id"], item["file_id"]
            )
            with handle:
                assert handle.read()
        restarted.release(manifest["manifest_id"])
        restarted.release(fresh["manifest_id"])
        assert not live_file_path(lake, a.file_id).exists()
        assert not live_file_path(lake, b.file_id).exists()
        assert restarted.recycle_retired() == 0
    finally:
        restarted.stop()


def test_release_is_idempotent_and_blocks_downloads(lake: Lake) -> None:
    lake.upload("b1", rows_for(1, 2))
    manifest = lake.registry.claim()
    file_id = manifest["files"][0]["file_id"]
    first = lake.registry.release(manifest["manifest_id"])
    second = lake.registry.release(manifest["manifest_id"])
    assert first["already_released"] is False
    assert second["already_released"] is True
    assert first["released_at"] == second["released_at"]
    with pytest.raises(Exception) as excinfo:
        lake.registry.open_for_download(manifest["manifest_id"], file_id)
    assert getattr(excinfo.value, "status_code", None) == 410


def test_old_file_is_recycled_only_after_every_manifest_releases(lake: Lake) -> None:
    lake.upload("b1", rows_for(1, 3))
    old_file = lake.registry.list_live()["files"][0].file_id
    first = lake.registry.claim()
    second = lake.registry.claim()
    lake.registry.merge()
    assert lake.registry.recycle_retired() == 0
    lake.registry.release(first["manifest_id"])
    assert lake.registry.recycle_retired() == 0
    old_path = live_file_path(lake, old_file)
    assert old_path.exists()
    lake.registry.release(second["manifest_id"])
    assert not old_path.exists()
    assert lake.registry.recycle_retired() == 0


def test_unregistered_merge_output_is_invisible_and_swept(lake: Lake) -> None:
    lake.upload("b1", rows_for(1, 2))
    orphan = lake.settings.batches_dir / "m_crashed.parquet"
    orphan.write_bytes(b"not a real parquet file")
    live_before = lake.registry.list_live()
    assert "m_crashed" not in {item.file_id for item in live_before["files"]}
    assert lake.registry.sweep_orphans() == 0  # within grace period
    assert orphan.exists()
    stale = time.time() - 3600
    os.utime(orphan, (stale, stale))
    assert lake.registry.sweep_orphans() == 1
    assert not orphan.exists()
    assert lake.registry.list_live() == live_before


def test_duplicate_batch_id_is_rejected(lake: Lake) -> None:
    first = lake.upload("b1", rows_for(1, 2))["file"]
    from app.errors import ConflictError

    with pytest.raises(ConflictError):
        lake.upload("b1", rows_for(9, 1))
    live = lake.registry.list_live()
    assert [item.file_id for item in live["files"]] == [first.file_id]
    assert live["row_count"] == 2
    # The rejected upload's Parquet must not linger as a leak.
    assert len(list(lake.settings.batches_dir.iterdir())) == 1


def test_every_manifest_is_an_exact_partition_of_ingested_rows(lake: Lake) -> None:
    """Concurrent claims/merges/uploads: each manifest holds every row once."""
    uploaded: list[tuple[str, str, float, float]] = []
    manifests: list[dict] = []
    failures: list[BaseException] = []
    lock = threading.Lock()
    stop = threading.Event()

    for index in range(4):
        rows = rows_for(index, 3, prefix="seed")
        lake.upload(f"seed-{index}", rows)
        uploaded.extend(rows)

    def worker(seed: int) -> None:
        rng = random.Random(seed)
        try:
            while not stop.is_set():
                action = rng.random()
                if action < 0.5:
                    manifest = lake.registry.claim(f"job-{seed}")
                    with lock:
                        manifests.append(manifest)
                elif action < 0.8:
                    lake.registry.merge()
                else:
                    lake.registry.list_live()
        except BaseException as exc:  # pragma: no cover - surfaced below
            failures.append(exc)

    threads = [threading.Thread(target=worker, args=(seed,)) for seed in range(3)]
    for thread in threads:
        thread.start()
    try:
        for index in range(4):
            rows = rows_for(index, 2, prefix="late")
            lake.upload(f"late-{index}", rows)
            with lock:
                uploaded.extend(rows)
            time.sleep(0.005)
    finally:
        stop.set()
        for thread in threads:
            thread.join()
    assert not failures
    assert manifests

    final_manifest = lake.registry.claim("final")
    final_rows = manifest_rows(lake, final_manifest)
    # Every ingested row (including exact duplicates) is present exactly once,
    # regardless of how concurrent merges interleaved the file order.
    assert sorted(final_rows) == sorted(norm(uploaded))
    assert len(final_rows) == len(uploaded)

    for manifest in manifests:
        rows = manifest_rows(lake, manifest)
        assert len(rows) == manifest["row_count"]
        assert len(rows) == len(set(rows))  # no row duplicated by merging
        assert set(rows) <= set(norm(uploaded))

    live = lake.registry.list_live()
    assert live["row_count"] == len(uploaded)


def norm(rows):
    """Canonicalise timestamp text so CSV input and Parquet output compare."""
    return [
        (vehicle, _canon_ts(value), lat, lon)
        for vehicle, value, lat, lon in rows
    ]


def _canon_ts(value) -> str:
    if isinstance(value, str):
        return _timestamp_text(_parse_ts(value))
    return _timestamp_text(value)


def _parse_ts(text: str):
    from datetime import UTC, datetime

    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


def _timestamp_text(value) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _digest(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()
