"""Runtime configuration.

All state lives under a single shared directory (``LAKE_ROOT``) so that two
service instances sharing a volume see exactly the same thing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    root: Path
    # Largest accepted CSV upload (bytes). 1000 trajectory rows are tiny; the
    # cap protects the API from oversized multipart bodies.
    max_upload_bytes: int = 16 * 1024 * 1024
    # Hard per-batch row limit from the ingestion contract.
    max_batch_rows: int = 1000
    # Age threshold before files that are not registered in the database are
    # treated as abandoned (left behind by a crashed writer).  This prevents a
    # maintenance sweep from racily deleting a file another instance has just
    # written but not yet registered.
    orphan_grace_s: float = 300.0
    # How long a merge waits for the cross-instance merge lock.
    merge_lock_timeout_s: float = 120.0
    # SQLite lock wait, in milliseconds.
    db_busy_timeout_ms: int = 30000
    # Background maintenance cadence: orphan sweep / deferred recycling.
    maintenance_interval_s: float = 30.0

    @property
    def batches_dir(self) -> Path:
        return self.root / "batches"

    @property
    def tmp_dir(self) -> Path:
        return self.root / "tmp"

    @property
    def leases_dir(self) -> Path:
        """Liveness leases for files that are written but not yet registered."""
        return self.root / "leases"

    @property
    def sweep_lock_path(self) -> Path:
        """Serialises orphan sweeps across instances (distinct from merges)."""
        return self.root / "sweep.lock"

    @property
    def db_path(self) -> Path:
        return self.root / "lake.sqlite3"

    @property
    def lock_path(self) -> Path:
        return self.root / "lake.lock"


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:  # pragma: no cover - startup misconfiguration
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def load_settings() -> Settings:
    return Settings(
        root=Path(os.environ.get("LAKE_ROOT", "/data")).resolve(),
        max_upload_bytes=int(
            _env_float("LAKE_MAX_UPLOAD_BYTES", 16 * 1024 * 1024)
        ),
        max_batch_rows=int(_env_float("LAKE_MAX_BATCH_ROWS", 1000)),
        orphan_grace_s=_env_float("LAKE_ORPHAN_GRACE_S", 300.0),
        merge_lock_timeout_s=_env_float("LAKE_MERGE_LOCK_TIMEOUT_S", 120.0),
        db_busy_timeout_ms=int(_env_float("LAKE_DB_BUSY_TIMEOUT_MS", 30000)),
        maintenance_interval_s=_env_float("LAKE_MAINTENANCE_INTERVAL_S", 30.0),
    )
