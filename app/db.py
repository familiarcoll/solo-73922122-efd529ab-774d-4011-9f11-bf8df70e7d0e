"""SQLite registry for the file list, merges and claimed manifests.

The database is the single source of truth for *which* Parquet files are
live.  Parquet files themselves are immutable.  Every state change that must
be visible atomically (publish a merged output + retire its inputs, snapshot a
manifest) happens in one ``BEGIN IMMEDIATE`` transaction, so two API
instances sharing the volume observe either the old or the new state, never a
mixture.
"""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .errors import ConflictError

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    id              TEXT NOT NULL UNIQUE,
    kind            TEXT NOT NULL CHECK (kind IN ('batch', 'merged')),
    rel_path        TEXT NOT NULL,
    rows            INTEGER NOT NULL CHECK (rows >= 0),
    bytes           INTEGER NOT NULL CHECK (bytes >= 0),
    sha256          TEXT NOT NULL,
    csv_sha256      TEXT,
    source_batch_id TEXT UNIQUE,
    merge_id        TEXT,
    created_at      TEXT NOT NULL,
    live            INTEGER NOT NULL DEFAULT 1 CHECK (live IN (0, 1)),
    retired_at      TEXT,
    retired_by      TEXT,
    recycled_at     TEXT
);

CREATE INDEX IF NOT EXISTS files_live_seq ON files (live, seq);
CREATE INDEX IF NOT EXISTS files_retired ON files (live, recycled_at);
CREATE UNIQUE INDEX IF NOT EXISTS files_merge_output
    ON files (merge_id) WHERE merge_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS merges (
    id             TEXT PRIMARY KEY,
    output_file_id TEXT NOT NULL UNIQUE REFERENCES files (id),
    input_count    INTEGER NOT NULL,
    row_count      INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    committed_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS merge_inputs (
    merge_id TEXT NOT NULL REFERENCES merges (id),
    file_id  TEXT NOT NULL REFERENCES files (id),
    ordinal  INTEGER NOT NULL,
    PRIMARY KEY (merge_id, file_id)
);

CREATE INDEX IF NOT EXISTS merge_inputs_file ON merge_inputs (file_id);

CREATE TABLE IF NOT EXISTS manifests (
    id          TEXT PRIMARY KEY,
    label       TEXT,
    state       TEXT NOT NULL CHECK (state IN ('open', 'released')),
    created_at  TEXT NOT NULL,
    released_at TEXT,
    file_count  INTEGER NOT NULL,
    row_count   INTEGER NOT NULL,
    bytes       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS manifests_state ON manifests (state, created_at);

CREATE TABLE IF NOT EXISTS manifest_files (
    manifest_id TEXT NOT NULL REFERENCES manifests (id),
    file_id     TEXT NOT NULL REFERENCES files (id),
    ordinal     INTEGER NOT NULL,
    rows        INTEGER NOT NULL,
    bytes       INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    PRIMARY KEY (manifest_id, file_id)
);

CREATE INDEX IF NOT EXISTS manifest_files_file
    ON manifest_files (file_id);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

SCHEMA_VERSION = "1"


def connect(db_path: Path, busy_timeout_ms: int) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path),
        timeout=busy_timeout_ms / 1000.0,
        isolation_level=None,  # explicit transactions only
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = FULL")
    return conn


def initialise(db_path: Path, busy_timeout_ms: int) -> None:
    """Create the schema. Safe to call from both instances concurrently."""
    conn = connect(db_path, busy_timeout_ms)
    try:
        # Switching journal mode needs a brief exclusive lock and ignores
        # busy_timeout, so two instances booting at once must retry it.  WAL
        # is persistent, so normally only the first boot actually changes it.
        mode = _retry_locked(lambda: _enable_wal(conn))
        if str(mode).lower() != "wal":
            # Still correct, just less concurrent. A local volume normally
            # supports WAL; networked filesystems are unsupported.
            print(f"warning: SQLite journal_mode is {mode!r}, not 'wal'")
        _retry_locked(lambda: _create_schema(conn))
    finally:
        conn.close()


def _enable_wal(conn: sqlite3.Connection) -> str:
    return conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]


def _create_schema(conn: sqlite3.Connection) -> None:
    # ``executescript`` would implicitly COMMIT the transaction, so run the
    # DDL statement by statement instead.  Every statement is idempotent,
    # which makes concurrent startup of two instances harmless.
    statements = [part.strip() for part in SCHEMA.split(";") if part.strip()]
    conn.execute("BEGIN IMMEDIATE")
    try:
        for statement in statements:
            conn.execute(statement)
        conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _retry_locked(fn, attempts: int = 8):
    delay = 0.05
    last: Exception | None = None
    for _ in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            last = exc
            time.sleep(delay)
            delay = min(delay * 2, 2.0)
    raise ConflictError(
        f"数据库持续忙碌，请稍后重试（{last}）"
    )


@contextmanager
def read_tx(conn: sqlite3.Connection) -> Iterator[None]:
    """Snapshot read. WAL readers do not block writers."""
    conn.execute("BEGIN")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def write_tx(conn: sqlite3.Connection) -> Iterator[None]:
    _begin_immediate(conn)
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _begin_immediate(conn: sqlite3.Connection) -> None:
    delay = 0.02
    last: Exception | None = None
    for _ in range(10):
        try:
            conn.execute("BEGIN IMMEDIATE")
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "locked" not in message and "busy" not in message:
                raise
            last = exc
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
    raise ConflictError(f"数据库瞬间繁忙，请重试（{last}）")
