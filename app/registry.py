"""Application service: ingestion, merge, claim, download and recycling.

Concurrency model
-----------------
* Parquet files are immutable and written under ``tmp`` then atomically
  renamed into ``batches``.
* SQLite (WAL, ``BEGIN IMMEDIATE`` for writers) is the linearisation point
  for anything that changes what is live or what a manifest holds.
* Merges take a cross-instance advisory file lock to avoid duplicate work.
  Correctness does not rely on it: the commit transaction re-checks that
  every snapshot input is still live and rolls back otherwise.
* A manifest pins file ids.  A file may be deleted only when it is not live
  and no *open* manifest references it.  Open manifests are never expired
  automatically, so a claimed list stays readable across merges, releases by
  other jobs and process restarts.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from . import db
from .config import Settings
from .csv_ingest import parse_csv
from .errors import (
    ConflictError,
    CorruptError,
    GoneError,
    NotFoundError,
    ValidationError,
)
from .leases import (
    LeaseConflict,
    UploadLease,
    lease_key_for_filename,
    lease_path,
    probe_lease,
)
from .models import FileInfo
from .parquet_io import new_file_id, now_iso, write_batch, write_merged

try:  # POSIX (the Docker image is Linux)
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]


class StaleInputs(Exception):
    """Merge snapshot lost an input before commit; retry the merge."""


class FileLock:
    """Advisory exclusive lock shared by every instance on the volume."""

    def __init__(self, path: Path, timeout_s: float):
        self.path = path
        self.timeout_s = timeout_s
        self._fh: BinaryIO | None = None

    def __enter__(self) -> FileLock:
        self._fh = open(self.path, "a+b")
        if fcntl is None:  # pragma: no cover - non-POSIX fallback
            return self
        deadline = time.monotonic() + self.timeout_s
        while True:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    self._fh.close()
                    self._fh = None
                    raise ConflictError(
                        "另一个合并正在进行，等待锁超时；本次合并未做任何改动，可重试"
                    ) from None
                time.sleep(0.05)

    def __exit__(self, *exc_info: object) -> None:
        if self._fh is not None:
            if fcntl is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            self._fh.close()
            self._fh = None


class Registry:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._maintenance_stop = threading.Event()
        self._maintenance_thread: threading.Thread | None = None
        self.instance_id = os.environ.get("HOSTNAME") or uuid.uuid4().hex[:12]

    # ---------------------------------------------------------------- setup
    def start(self) -> None:
        root = self.settings.root
        for path in (
            root,
            self.settings.batches_dir,
            self.settings.tmp_dir,
            self.settings.leases_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.settings.lock_path.touch(exist_ok=True)
        self.settings.sweep_lock_path.touch(exist_ok=True)
        db.initialise(self.settings.db_path, self.settings.db_busy_timeout_ms)
        try:
            self.run_maintenance()
        except Exception as exc:  # pragma: no cover - startup resilience
            print(f"warning: startup maintenance failed: {exc}")
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name="lake-maintenance",
            daemon=True,
        )
        self._maintenance_thread.start()

    def stop(self) -> None:
        self._maintenance_stop.set()
        thread = self._maintenance_thread
        if thread is not None:
            thread.join(timeout=5)
            self._maintenance_thread = None

    def _maintenance_loop(self) -> None:
        while not self._maintenance_stop.wait(
            self.settings.maintenance_interval_s
        ):
            try:
                self.run_maintenance()
            except Exception as exc:  # pragma: no cover - background best effort
                print(f"warning: maintenance failed: {exc}")

    def run_maintenance(self) -> dict[str, int]:
        # Sweeps are serialised against each other (across instances) with a
        # lock that is *only* for the sweep.  They are never serialised
        # against uploads or whole merges: an unregistered writer's file is
        # protected by its upload lease, and a registered file by the atomic
        # conditional UPDATE in recycle_retired.  Per-file deletion takes the
        # merge lock non-blocking with a short timeout and simply defers on
        # contention, so a paused upload, merge or peer cleanup can never
        # stall another instance's maintenance or its traffic.
        try:
            with FileLock(self.settings.sweep_lock_path, 0.5):
                recycled = self.recycle_retired()
                swept = self.sweep_orphans()
        except ConflictError:
            return {"recycled": 0, "orphans_removed": 0, "skipped": True}
        return {"recycled": recycled, "orphans_removed": swept}

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = db.connect(
            self.settings.db_path, self.settings.db_busy_timeout_ms
        )
        try:
            yield conn
        finally:
            conn.close()

    # -------------------------------------------------------------- helpers
    @staticmethod
    def _file_info(row: sqlite3.Row) -> FileInfo:
        return FileInfo(
            file_id=row["id"],
            seq=row["seq"],
            kind=row["kind"],
            rows=row["rows"],
            bytes=row["bytes"],
            sha256=row["sha256"],
            created_at=row["created_at"],
            source_batch_id=row["source_batch_id"],
            merge_id=row["merge_id"],
        )

    @staticmethod
    def _select_live(conn: sqlite3.Connection) -> list[sqlite3.Row]:
        return conn.execute(
            """
            SELECT seq, id, kind, rel_path, rows, bytes, sha256, created_at,
                   source_batch_id, merge_id
              FROM files
             WHERE live = 1
             ORDER BY seq
            """
        ).fetchall()

    def _abs(self, rel_path: str) -> Path:
        # rel_path is always produced by this service ("batches/<id>.parquet").
        candidate = (self.settings.root / rel_path).resolve()
        if self.settings.root not in candidate.parents:
            raise CorruptError("文件路径越出数据根目录")
        return candidate

    # ------------------------------------------------------------- ingestion
    def ingest_batch(self, batch_id: str, payload: bytes) -> dict:
        batch_id = self._validate_batch_id(batch_id)
        if not payload:
            raise ValidationError("上传内容为空")
        if len(payload) > self.settings.max_upload_bytes:
            raise ValidationError(
                f"上传内容超过 {self.settings.max_upload_bytes} 字节上限"
            )
        parsed = parse_csv(payload, max_rows=self.settings.max_batch_rows)
        csv_sha256 = hashlib.sha256(payload).hexdigest()
        file_id = new_file_id("b")
        with UploadLease(self.settings.leases_dir, file_id):
            # The lease is taken before the first byte is written and released
            # only after the registry commit (or after an explicit discard).
            # A paused upload therefore stays protected for as long as it
            # pauses; a crashed upload releases the lease automatically.
            written = write_batch(
                tmp_dir=self.settings.tmp_dir,
                batches_dir=self.settings.batches_dir,
                table=parsed.table,
                batch_id=batch_id,
                file_id=file_id,
            )
            created_at = now_iso()
            try:
                with self.connection() as conn, db.write_tx(conn):
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
                            csv_sha256,
                            batch_id,
                            created_at,
                        ),
                    )
                    row = conn.execute(
                        """
                            SELECT seq, id, kind, rel_path, rows, bytes, sha256,
                                   created_at, source_batch_id, merge_id
                              FROM files WHERE id = ?
                            """,
                        (file_id,),
                    ).fetchone()
            except sqlite3.IntegrityError:
                # The unique batch id was already ingested.  Our file was never
                # registered, so removing it cannot affect any valid state.
                self._discard_unregistered(file_id, written.rel_path)
                existing = self._find_batch(batch_id)
                suffix = (
                    f"；已有文件 {existing.file_id}（{existing.rows} 行）"
                    if existing
                    else ""
                )
                raise ConflictError(
                    f"批号 {batch_id!r} 已上传过{suffix}"
                ) from None
            except BaseException:
                # Disk write succeeded but registration failed: if the row was
                # not committed the file is invisible, so removing it is safe.
                # Verify first in case the commit did land and a later error
                # was raised (deleting a live file would be catastrophic).
                self._discard_unregistered(file_id, written.rel_path)
                raise
        return {"batch_id": batch_id, "file": self._file_info(row)}

    def _discard_unregistered(self, file_id: str, rel_path: str) -> None:
        try:
            with self.connection() as conn, db.read_tx(conn):
                row = conn.execute(
                    "SELECT 1 FROM files WHERE id = ?", (file_id,)
                ).fetchone()
        except Exception as exc:  # pragma: no cover - DB unavailable
            # Cannot confirm; leave the file for the orphan sweep rather than
            # risk deleting a registered one.
            print(f"warning: could not verify registration of {file_id}: {exc}")
            return
        if row is None:
            self._abs(rel_path).unlink(missing_ok=True)

    def _find_batch(self, batch_id: str) -> FileInfo | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT seq, id, kind, rel_path, rows, bytes, sha256, created_at,
                       source_batch_id, merge_id
                  FROM files WHERE source_batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
        return self._file_info(row) if row else None

    @staticmethod
    def _validate_batch_id(batch_id: str) -> str:
        value = (batch_id or "").strip()
        if not value:
            raise ValidationError("批号不能为空")
        if len(value) > 128:
            raise ValidationError("批号最多 128 个字符")
        if any(ord(ch) < 0x20 for ch in value):
            raise ValidationError("批号不能包含控制字符")
        return value

    def get_file(self, file_id: str) -> FileInfo:
        with self.connection() as conn, db.read_tx(conn):
            row = conn.execute(
                """
                    SELECT seq, id, kind, rel_path, rows, bytes, sha256,
                           created_at, source_batch_id, merge_id
                      FROM files WHERE id = ?
                    """,
                (file_id,),
            ).fetchone()
        if row is None:
            raise NotFoundError(f"文件 {file_id!r} 不存在")
        return self._file_info(row)

    # ------------------------------------------------------------ live files
    def list_live(self) -> dict:
        with self.connection() as conn, db.read_tx(conn):
            rows = self._select_live(conn)
        files = [self._file_info(row) for row in rows]
        return {
            "files": files,
            "file_count": len(files),
            "row_count": sum(item.rows for item in files),
        }

    # ----------------------------------------------------------------- merge
    def merge(self) -> dict:
        started_at = now_iso()
        merge_id = f"mg_{uuid.uuid4().hex}"
        with FileLock(
            self.settings.lock_path, self.settings.merge_lock_timeout_s
        ):
            for _attempt in range(5):
                with self.connection() as conn, db.read_tx(conn):
                    rows = self._select_live(conn)
                    inputs = [
                        (r["id"], self._abs(r["rel_path"]), r["rows"])
                        for r in rows
                    ]
                if not inputs:
                    raise ConflictError("当前没有现行文件，无需合并")
                expected_rows = sum(item[2] for item in inputs)
                file_id = new_file_id("m")
                with UploadLease(self.settings.leases_dir, file_id):
                    # The lease spans publish + commit, so a sweep can never
                    # mistake a slow merge's unregistered output for an
                    # abandoned file.  It is released automatically if this
                    # process dies mid-merge.
                    try:
                        written = write_merged(
                            tmp_dir=self.settings.tmp_dir,
                            batches_dir=self.settings.batches_dir,
                            inputs=[
                                (file_id, path) for file_id, path, _ in inputs
                            ],
                            merge_id=merge_id,
                            file_id=file_id,
                        )
                    except FileNotFoundError:
                        # Extremely defensive: an input vanished mid-merge.
                        # Do not publish; retry from a fresh snapshot.
                        time.sleep(0.05)
                        continue
                    if written.row_count != expected_rows:
                        self._abs(written.rel_path).unlink(missing_ok=True)
                        raise CorruptError(
                            "合并输出的行数与输入行数不一致，已放弃本次合并结果"
                        )
                    committed_at = now_iso()
                    try:
                        with self.connection() as conn:
                            self._commit_merge(
                                conn,
                                merge_id=merge_id,
                                started_at=started_at,
                                committed_at=committed_at,
                                file_id=file_id,
                                written=written,
                                input_ids=[item[0] for item in inputs],
                            )
                    except StaleInputs:
                        self._discard_unregistered(file_id, written.rel_path)
                        time.sleep(0.05)
                        continue
                    except BaseException:
                        # Re-check before deleting: if COMMIT actually landed
                        # and the error came afterwards, the output is live and
                        # must never be removed.
                        self._discard_unregistered(file_id, written.rel_path)
                        raise
                try:
                    self.recycle_retired(merge_lock_held=True)
                except Exception as exc:  # best effort after a successful commit
                    print(f"warning: recycling after merge failed: {exc}")
                return {
                    "merge_id": merge_id,
                    "output_file_id": file_id,
                    "input_file_ids": [item[0] for item in inputs],
                    "input_count": len(inputs),
                    "row_count": written.row_count,
                    "committed_at": committed_at,
                }
        raise ConflictError("合并输入持续变化，未产生结果；请重试")

    def _commit_merge(
        self,
        conn: sqlite3.Connection,
        *,
        merge_id: str,
        started_at: str,
        committed_at: str,
        file_id: str,
        written,
        input_ids: list[str],
    ) -> None:
        with db.write_tx(conn):
            conn.execute(
                """
                INSERT INTO files
                    (id, kind, rel_path, rows, bytes, sha256, csv_sha256,
                     source_batch_id, merge_id, created_at, live)
                VALUES (?, 'merged', ?, ?, ?, ?, NULL, NULL, ?, ?, 1)
                """,
                (
                    file_id,
                    written.rel_path,
                    written.row_count,
                    written.byte_size,
                    written.sha256,
                    merge_id,
                    committed_at,
                ),
            )
            conn.execute(
                """
                INSERT INTO merges
                    (id, output_file_id, input_count, row_count, created_at,
                     committed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    merge_id,
                    file_id,
                    len(input_ids),
                    written.row_count,
                    started_at,
                    committed_at,
                ),
            )
            for ordinal, input_id in enumerate(input_ids):
                conn.execute(
                    """
                    INSERT INTO merge_inputs (merge_id, file_id, ordinal)
                    VALUES (?, ?, ?)
                    """,
                    (merge_id, input_id, ordinal),
                )
                updated = conn.execute(
                    """
                    UPDATE files
                       SET live = 0, retired_at = ?, retired_by = ?
                     WHERE id = ? AND live = 1
                    """,
                    (committed_at, merge_id, input_id),
                ).rowcount
                if updated != 1:
                    raise StaleInputs(input_id)
            # The commit below is the single moment at which the new file list
            # becomes visible.  A crash before it leaves only an unregistered
            # Parquet file, which the orphan sweep removes.

    # -------------------------------------------------------------- manifests
    def claim(self, label: str | None = None) -> dict:
        manifest_id = f"mf_{uuid.uuid4().hex}"
        created_at = now_iso()
        if label is not None:
            label = label.strip()[:200] or None
        with self.connection() as conn:
            with db.write_tx(conn):
                rows = self._select_live(conn)
                if not rows:
                    raise ConflictError("当前没有现行文件可领取")
                total_rows = sum(r["rows"] for r in rows)
                total_bytes = sum(r["bytes"] for r in rows)
                conn.execute(
                    """
                    INSERT INTO manifests
                        (id, label, state, created_at, released_at, file_count,
                         row_count, bytes)
                    VALUES (?, ?, 'open', ?, NULL, ?, ?, ?)
                    """,
                    (
                        manifest_id,
                        label,
                        created_at,
                        len(rows),
                        total_rows,
                        total_bytes,
                    ),
                )
                conn.executemany(
                    """
                    INSERT INTO manifest_files
                        (manifest_id, file_id, ordinal, rows, bytes, sha256)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            manifest_id,
                            row["id"],
                            ordinal,
                            row["rows"],
                            row["bytes"],
                            row["sha256"],
                        )
                        for ordinal, row in enumerate(rows, start=1)
                    ],
                )
            with db.read_tx(conn):
                return self._manifest_payload(conn, manifest_id)

    def _manifest_payload(
        self,
        conn: sqlite3.Connection,
        manifest_id: str,
        message: str | None = None,
    ) -> dict:
        manifest = conn.execute(
            """
            SELECT id, label, state, created_at, released_at, file_count,
                   row_count, bytes
              FROM manifests WHERE id = ?
            """,
            (manifest_id,),
        ).fetchone()
        if manifest is None:
            raise NotFoundError(f"清单 {manifest_id!r} 不存在")
        files = conn.execute(
            """
            SELECT ordinal, file_id, rows, bytes, sha256
              FROM manifest_files
             WHERE manifest_id = ?
             ORDER BY ordinal
            """,
            (manifest_id,),
        ).fetchall()
        return {
            "manifest_id": manifest["id"],
            "label": manifest["label"],
            "state": manifest["state"],
            "created_at": manifest["created_at"],
            "released_at": manifest["released_at"],
            "file_count": manifest["file_count"],
            "row_count": manifest["row_count"],
            "bytes": manifest["bytes"],
            "files": [
                {
                    "ordinal": row["ordinal"],
                    "file_id": row["file_id"],
                    "rows": row["rows"],
                    "bytes": row["bytes"],
                    "sha256": row["sha256"],
                    "download_path": (
                        f"/v1/manifests/{manifest_id}/files/"
                        f"{row['file_id']}/download"
                    ),
                }
                for row in files
            ],
            "message": message,
        }

    def get_manifest(self, manifest_id: str) -> dict:
        with self.connection() as conn, db.read_tx(conn):
            return self._manifest_payload(conn, manifest_id)

    def release(self, manifest_id: str) -> dict:
        released_at = now_iso()
        with self.connection() as conn, db.write_tx(conn):
            row = conn.execute(
                "SELECT state, released_at FROM manifests WHERE id = ?",
                (manifest_id,),
            ).fetchone()
            if row is None:
                raise NotFoundError(f"清单 {manifest_id!r} 不存在")
            already = row["state"] == "released"
            if already:
                released_at = row["released_at"] or released_at
            else:
                conn.execute(
                    """
                        UPDATE manifests
                           SET state = 'released', released_at = ?
                         WHERE id = ? AND state = 'open'
                        """,
                    (released_at, manifest_id),
                )
        if not already:
            # Now that the pin is gone, retired inputs may be reclaimed.
            self.recycle_retired()
        return {
            "manifest_id": manifest_id,
            "state": "released",
            "released_at": released_at,
            "already_released": already,
            "message": (
                "清单此前已释放，本次为幂等确认"
                if already
                else "清单已释放；不再被任何未释放清单引用的旧文件已进入回收流程"
            ),
        }

    # --------------------------------------------------------------- download
    def open_for_download(
        self, manifest_id: str, file_id: str
    ) -> tuple[BinaryIO, int, str, str]:
        """Return an already-open descriptor, pinning bytes across unlink.

        The descriptor is opened *after* confirming the manifest is still
        open.  Once open, a concurrent release + recycling may unlink the
        name, but POSIX keeps the bytes readable through the descriptor.
        """
        for _ in range(5):
            with self.connection() as conn, db.read_tx(conn):
                path = self._download_path(conn, manifest_id, file_id)
                row = conn.execute(
                    "SELECT bytes, sha256 FROM manifest_files "
                    "WHERE manifest_id = ? AND file_id = ?",
                    (manifest_id, file_id),
                ).fetchone()
            try:
                fd = os.open(path, os.O_RDONLY)
            except FileNotFoundError:
                # It may have been released + recycled between our read and
                # open; the retry observes the new manifest state.
                time.sleep(0.02)
                continue
            handle = os.fdopen(fd, "rb")
            return handle, int(row["bytes"]), str(row["sha256"]), path.name
        raise GoneError("清单或文件状态已变化，请重新读取清单")

    def _download_path(
        self, conn: sqlite3.Connection, manifest_id: str, file_id: str
    ) -> Path:
        manifest = conn.execute(
            "SELECT state FROM manifests WHERE id = ?",
            (manifest_id,),
        ).fetchone()
        if manifest is None:
            raise NotFoundError(f"清单 {manifest_id!r} 不存在")
        if manifest["state"] != "open":
            raise GoneError(
                "清单已释放，清单内文件不再提供下载；请重新领取现行清单"
            )
        row = conn.execute(
            """
            SELECT f.rel_path
              FROM manifest_files mf
              JOIN files f ON f.id = mf.file_id
             WHERE mf.manifest_id = ? AND mf.file_id = ?
            """,
            (manifest_id, file_id),
        ).fetchone()
        if row is None:
            raise NotFoundError(
                f"文件 {file_id!r} 不在清单 {manifest_id!r} 中"
            )
        return self._abs(row["rel_path"])

    def resolve_manifest_file(
        self, manifest_id: str, file_id: str
    ) -> tuple[str, int, str]:
        """Metadata-only lookup used by tests and diagnostics."""
        with self.connection() as conn, db.read_tx(conn):
            row = conn.execute(
                """
                    SELECT f.rel_path, mf.bytes, mf.sha256
                      FROM manifest_files mf
                      JOIN files f ON f.id = mf.file_id
                     WHERE mf.manifest_id = ? AND mf.file_id = ?
                    """,
                (manifest_id, file_id),
            ).fetchone()
            if row is None:
                raise NotFoundError("清单中不存在该文件")
            return row["rel_path"], row["bytes"], row["sha256"]

    # ----------------------------------------------------------- maintenance
    def recycle_retired(self, *, merge_lock_held: bool = False) -> int:
        """Mark and physically remove retired files no open manifest pins.

        A file's liveness and a manifest's pin can only change in a
        ``BEGIN IMMEDIATE`` transaction (merge commit / claim).  Each
        candidate is therefore claimed with one atomic conditional UPDATE
        that re-checks ``live = 0``, ``recycled_at IS NULL`` and "no open
        manifest references it"; only the winner of that update unlinks the
        path, making concurrent recyclers idempotent.

        Deletion is additionally guarded by the merge lock (taken once per
        pass, non-blocking, short timeout): a merge reads its input Parquets
        after snapshotting them, and another merge may have retired those
        inputs in the meantime.  On contention the whole recycle step defers
        to the next pass, so a paused merge only postpones removal of already
        retired files -- never uploads, claims, downloads or the sweep of
        unregistered orphans (which runs before/independently).
        """
        now = now_iso()
        with self.connection() as conn, db.read_tx(conn):
            candidates = conn.execute(
                """
                    SELECT f.id, f.rel_path
                      FROM files f
                     WHERE f.live = 0
                       AND f.recycled_at IS NULL
                       AND NOT EXISTS (
                           SELECT 1
                             FROM manifest_files mf
                             JOIN manifests m ON m.id = mf.manifest_id
                            WHERE mf.file_id = f.id
                              AND m.state = 'open'
                       )
                     ORDER BY f.seq
                     LIMIT 500
                    """
            ).fetchall()
        if not candidates:
            return 0
        if merge_lock_held:
            return self._recycle_candidates(candidates, now)
        try:
            # One short, non-blocking acquisition per pass: if a merge is
            # running, the whole recycle step is deferred to the next pass
            # (never more than 0.5 s of waiting here).
            with FileLock(self.settings.lock_path, 0.5):
                return self._recycle_candidates(candidates, now)
        except ConflictError:
            return 0

    def _recycle_candidates(self, candidates, reclaimed_at: str) -> int:
        removed = 0
        for row in candidates:
            if self._claim_and_unlink(row["id"], row["rel_path"], reclaimed_at):
                removed += 1
        return removed

    def _claim_and_unlink(
        self, file_id: str, rel_path: str, reclaimed_at: str
    ) -> bool:
        # Claim the row under BEGIN IMMEDIATE: only the winner of this update
        # may unlink the path, which makes concurrent recyclers idempotent.
        with self.connection() as conn:
            with db.write_tx(conn):
                claimed = conn.execute(
                    """
                    UPDATE files
                       SET recycled_at = ?
                     WHERE id = ? AND live = 0 AND recycled_at IS NULL
                       AND NOT EXISTS (
                           SELECT 1
                             FROM manifest_files mf
                             JOIN manifests m ON m.id = mf.manifest_id
                            WHERE mf.file_id = files.id
                              AND m.state = 'open'
                       )
                    """,
                    (reclaimed_at, file_id),
                ).rowcount
        if claimed != 1:
            return False
        path = self._abs(rel_path)
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:  # pragma: no cover - disk-level failure
            # The row is already marked; a retry cannot see it again.  Put
            # the claim back so the next pass retries the deletion.
            with self.connection() as conn, db.write_tx(conn):
                conn.execute(
                    "UPDATE files SET recycled_at = NULL WHERE id = ?",
                    (file_id,),
                )
            print(f"warning: could not recycle {rel_path}: {exc}")
            return False
        return True

    def sweep_orphans(self) -> int:
        """Remove abandoned files: unregistered Parquet and stale temp files.

        An unregistered Parquet is deleted only if no live writer holds its
        upload lease, the registry re-checked under that lease still has no
        row for it, and it is older than the grace period.  The lease check is
        what protects a slow upload: age is no longer used as evidence that a
        writer is dead.

        The registry snapshot is taken *before* the scan, so a file registered
        between snapshot and scan is a sweep candidate; the lease re-check
        after the writer finishes (and the in-scan re-check) prevents it from
        being deleted.  If the sweep itself dies between unlink and bookkeeping
        the next pass simply finishes the job: unregistered files are
        reclaimed again, and recycled-registry entries are skipped by name.
        """
        now = time.time()
        grace = self.settings.orphan_grace_s
        known: set[str] = set()
        recycled: set[str] = set()
        with self.connection() as conn, db.read_tx(conn):
            for row in conn.execute(
                "SELECT rel_path, recycled_at FROM files"
            ):
                rel = row["rel_path"]
                if row["recycled_at"] is None:
                    known.add(rel)
                else:
                    recycled.add(rel)
        removed = 0

        def scan_dirs() -> int:
            removed_here = 0
            if self.settings.batches_dir.exists():
                for path in self.settings.batches_dir.iterdir():
                    if not path.is_file():
                        continue
                    rel = str(path.relative_to(self.settings.root))
                    if rel in known:
                        continue
                    if rel not in recycled:
                        try:
                            age = now - path.stat().st_mtime
                        except FileNotFoundError:
                            continue
                        if age < grace:
                            continue
                    if not self._sweep_candidate_is_abandoned(path):
                        continue
                    try:
                        path.unlink()
                        removed_here += 1
                    except FileNotFoundError:
                        pass
                    except OSError as exc:  # pragma: no cover
                        print(f"warning: could not remove orphan {rel}: {exc}")
            if self.settings.tmp_dir.exists():
                for path in self.settings.tmp_dir.iterdir():
                    try:
                        if not path.is_file():
                            continue
                        if now - path.stat().st_mtime < grace:
                            continue
                        if not self._sweep_candidate_is_abandoned(path):
                            continue
                        path.unlink()
                        removed_here += 1
                    except FileNotFoundError:
                        pass
                    except OSError as exc:  # pragma: no cover
                        print(f"warning: could not remove temp file {path}: {exc}")
            return removed_here

        # The scan is idempotent: if a previous sweep died half-way the next
        # pass simply redoes it.  Dead leases left after their data file was
        # already removed are finished off in the same pass.
        removed += scan_dirs()
        removed += self._sweep_dead_leases(now, grace)
        return removed

    def _sweep_dead_leases(self, now: float, grace: float) -> int:
        """Finish a cleanup that died after removing the data file.

        A lease whose data file is already gone (and whose writer is no
        longer alive) is pure leftover: the previous sweep may have been
        killed between ``unlink`` and lease cleanup.  Removing it lets the
        next pass converge without extending any grace period.  A lease that
        still has a data file is handled by the file scan above.
        """
        if not self.settings.leases_dir.exists():
            return 0
        removed = 0
        for lease in self.settings.leases_dir.iterdir():
            if not lease.is_file():
                continue
            key = lease_key_for_filename(lease.name)
            if (
                (self.settings.batches_dir / f"{key}.parquet").exists()
                or (self.settings.tmp_dir / f"{key}.parquet.tmp").exists()
            ):
                continue
            try:
                if now - lease.stat().st_mtime < grace:
                    continue
            except FileNotFoundError:
                continue
            fd, writer_alive = probe_lease(self.settings.leases_dir, key)
            if writer_alive:
                continue
            try:
                lease.unlink(missing_ok=True)
                removed += 1
            except OSError:  # pragma: no cover
                pass
            finally:
                if fd is not None:
                    os.close(fd)
        return removed

    def _sweep_candidate_is_abandoned(self, path: Path) -> bool:
        """Decide whether an unregistered-looking path may be deleted.

        Caller has already applied the grace-period/mtime pre-filter.  The
        authoritative check is done while holding the file's lease, so no
        writer can start on the name in between, and the registry is re-read
        to catch a registration committed after the sweep's snapshot.
        """
        key = lease_key_for_filename(path.name)
        fd, writer_alive = probe_lease(self.settings.leases_dir, key)
        if writer_alive:
            # A live upload/merge owns this name, no matter how long it has
            # paused.  Its lease disappears automatically if it exits.
            return False
        try:
            rel = str(path.relative_to(self.settings.root))
            with self.connection() as conn, db.read_tx(conn):
                row = conn.execute(
                    "SELECT recycled_at FROM files WHERE rel_path = ?",
                    (rel,),
                ).fetchone()
            if row is not None and row["recycled_at"] is None:
                # Registered successfully (possibly after the snapshot was
                # taken).  Never delete a registered file.
                return False
            return True
        finally:
            if fd is not None:
                # We hold the lease and its owner is gone, so the lease file
                # itself is dead.  Remove it; the unique file id is never
                # reused, so a later writer cannot need it.
                try:
                    os.unlink(lease_path(self.settings.leases_dir, key))
                except FileNotFoundError:
                    pass
                except OSError:  # pragma: no cover
                    pass
                os.close(fd)

    # ------------------------------------------------------------------ stats
    def stats(self) -> dict:
        with self.connection() as conn, db.read_tx(conn):
            live_files = conn.execute(
                "SELECT COUNT(*) FROM files WHERE live = 1"
            ).fetchone()[0]
            live_rows = conn.execute(
                "SELECT COALESCE(SUM(rows), 0) FROM files WHERE live = 1"
            ).fetchone()[0]
            retired = conn.execute(
                "SELECT COUNT(*) FROM files WHERE live = 0"
            ).fetchone()[0]
            recycled = conn.execute(
                "SELECT COUNT(*) FROM files WHERE recycled_at IS NOT NULL"
            ).fetchone()[0]
            open_manifests = conn.execute(
                "SELECT COUNT(*) FROM manifests WHERE state = 'open'"
            ).fetchone()[0]
            released = conn.execute(
                "SELECT COUNT(*) FROM manifests WHERE state = 'released'"
            ).fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        return {
            "live_files": live_files,
            "live_rows": live_rows,
            "retired_files": retired,
            "recycled_files": recycled,
            "open_manifests": open_manifests,
            "released_manifests": released,
            "total_files": total,
        }
