"""Parquet schema, atomic writers and readers for trajectory files.

Files are immutable once published.  A writer always produces a temporary
file inside ``LAKE_ROOT/tmp``, fsyncs it, then renames it into
``LAKE_ROOT/batches``.  The rename is atomic on a POSIX filesystem, so a
reader never observes a partially written file.  Publishing to the live file
list happens separately, by inserting a row into SQLite: until that commit
succeeds the file is invisible to every API operation.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA_VERSION = "1"

TRAJECTORY_SCHEMA = pa.schema(
    [
        pa.field("vehicle_id", pa.string(), nullable=False),
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("latitude", pa.float64(), nullable=False),
        pa.field("longitude", pa.float64(), nullable=False),
    ],
    metadata={b"lake.schema_version": SCHEMA_VERSION.encode()},
)


@dataclass(frozen=True)
class WrittenFile:
    rel_path: str
    byte_size: int
    sha256: str
    row_count: int


def new_file_id(prefix: str) -> str:
    """Generate a globally unique file id (``b_...`` or ``m_...``).

    The id is chosen by the caller so it can take a cross-instance upload
    lease on it *before* the first byte is written.
    """
    return f"{prefix}_{uuid.uuid4().hex}"


def utcnow() -> datetime:
    return datetime.now(UTC)


def isoformat_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def now_iso() -> str:
    return isoformat_utc(utcnow())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _publish(tmp_path: Path, final_dir: Path, file_id: str) -> Path:
    final_path = final_dir / f"{file_id}.parquet"
    _fsync_file(tmp_path)
    os.replace(tmp_path, final_path)
    _fsync_dir(final_dir)
    return final_path


def write_batch(
    *,
    tmp_dir: Path,
    batches_dir: Path,
    table: pa.Table,
    batch_id: str,
    file_id: str,
) -> WrittenFile:
    metadata = dict(TRAJECTORY_SCHEMA.metadata or {})
    metadata.update(
        {
            b"lake.kind": b"batch",
            b"lake.batch_id": batch_id.encode("utf-8"),
            b"lake.created_at": now_iso().encode(),
        }
    )
    table = table.replace_schema_metadata(metadata)
    tmp_path = tmp_dir / f"{file_id}.parquet.tmp"
    try:
        pq.write_table(table, tmp_path, compression="zstd", version="2.6")
        final_path = _publish(tmp_path, batches_dir, file_id)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        finally:
            raise
    written = WrittenFile(
        rel_path=str(final_path.relative_to(batches_dir.parent)),
        byte_size=final_path.stat().st_size,
        sha256=sha256_file(final_path),
        row_count=table.num_rows,
    )
    return written


def read_parquet(path: Path) -> pa.Table:
    return pq.read_table(path)


def write_merged(
    *,
    tmp_dir: Path,
    batches_dir: Path,
    inputs: list[tuple[str, Path]],
    merge_id: str,
    file_id: str,
) -> WrittenFile:
    """Concatenate immutable inputs into one new file.

    Row order follows the input order, and no row de-duplication happens:
    identical rows uploaded more than once stay present exactly as often as
    they were ingested.
    """
    tables: list[pa.Table] = []
    for _, path in inputs:
        table = read_parquet(path)
        # Schema metadata is per-file bookkeeping; strip it so tables from
        # batch/merge files concatenate cleanly.
        tables.append(table.replace_schema_metadata(None))
    if not tables:
        raise ValueError("cannot merge zero files")
    merged = pa.concat_tables(tables, promote_options="none")
    metadata = dict(TRAJECTORY_SCHEMA.metadata or {})
    metadata.update(
        {
            b"lake.kind": b"merged",
            b"lake.merge_id": merge_id.encode(),
            b"lake.created_at": now_iso().encode(),
            b"lake.input_count": str(len(inputs)).encode(),
        }
    )
    # Keep the declared metadata small and bounded: the authoritative input
    # list lives in SQLite (merge_inputs), not in the file header.
    merged = merged.replace_schema_metadata(metadata)
    tmp_path = tmp_dir / f"{file_id}.parquet.tmp"
    try:
        pq.write_table(merged, tmp_path, compression="zstd", version="2.6")
        final_path = _publish(tmp_path, batches_dir, file_id)
    except BaseException:
        try:
            tmp_path.unlink(missing_ok=True)
        finally:
            raise
    written = WrittenFile(
        rel_path=str(final_path.relative_to(batches_dir.parent)),
        byte_size=final_path.stat().st_size,
        sha256=sha256_file(final_path),
        row_count=merged.num_rows,
    )
    return written


def merge_ids_metadata(input_ids: list[str]) -> str:
    """Helper used in logs/tests; input ids are persisted in SQLite."""
    return json.dumps(input_ids, separators=(",", ":"))
