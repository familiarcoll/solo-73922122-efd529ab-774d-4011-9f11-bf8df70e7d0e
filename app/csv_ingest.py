"""Strict CSV parsing for one uploaded trajectory batch.

Contract: a UTF-8 CSV with a header row and at most ``max_rows`` data rows.
Required columns are vehicle id, UTC timestamp and WGS-84 latitude/longitude.
Rows are never de-duplicated or reordered: a repeated row is a repeated
observation and is preserved verbatim.
"""

from __future__ import annotations

import csv
import io
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

import pyarrow as pa

from .errors import ValidationError
from .parquet_io import TRAJECTORY_SCHEMA

# Accepted header spellings, normalised to lowercase and stripped.
_COLUMN_ALIASES: dict[str, str] = {}
for canonical, aliases in {
    "vehicle_id": (
        "vehicle_id",
        "vehicle",
        "vehicle_no",
        "vehicle_number",
        "device_id",
        "车辆号",
        "车辆编号",
        "车牌号",
    ),
    "timestamp": (
        "timestamp",
        "time",
        "utc_time",
        "utc_timestamp",
        "datetime",
        "时刻",
        "时间",
        "utc时刻",
    ),
    "latitude": ("latitude", "lat", "纬度"),
    "longitude": ("longitude", "lon", "lng", "经度"),
}.items():
    for alias in aliases:
        _COLUMN_ALIASES[alias.lower()] = canonical


@dataclass(frozen=True)
class ParsedBatch:
    table: pa.Table
    row_count: int


def _parse_timestamp(raw: str, row_no: int) -> datetime:
    text = raw.strip()
    if not text:
        raise ValidationError(f"第 {row_no} 行：timestamp 为空")
    candidate = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        value = datetime.fromisoformat(candidate)
    except ValueError:
        raise ValidationError(
            f"第 {row_no} 行：timestamp {text!r} 不是 ISO 8601 时刻"
        ) from None
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValidationError(
            f"第 {row_no} 行：timestamp {text!r} 缺少时区，必须为 UTC 时刻"
        )
    return value.astimezone(UTC)


def _parse_coordinate(raw: str, name: str, row_no: int, limit: float) -> float:
    text = raw.strip()
    if not text:
        raise ValidationError(f"第 {row_no} 行：{name} 为空")
    try:
        value = float(text)
    except ValueError:
        raise ValidationError(
            f"第 {row_no} 行：{name} {text!r} 不是数字"
        ) from None
    if not math.isfinite(value):
        raise ValidationError(f"第 {row_no} 行：{name} 必须是有限数值")
    if not -limit <= value <= limit:
        raise ValidationError(
            f"第 {row_no} 行：{name} {value} 超出 [-{limit:g}, {limit:g}]"
        )
    return value


def parse_csv(
    payload: bytes, *, max_rows: int
) -> ParsedBatch:
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValidationError(f"CSV 必须是 UTF-8 编码：{exc}") from None

    reader = csv.reader(io.StringIO(text, newline=""))
    try:
        header_raw = next(reader)
    except StopIteration:
        raise ValidationError("CSV 为空，缺少表头") from None

    header = [_COLUMN_ALIASES.get(col.strip().lower()) for col in header_raw]
    required = ["vehicle_id", "timestamp", "latitude", "longitude"]
    missing = [name for name in required if name not in header]
    if missing:
        raise ValidationError(
            "CSV 缺少必需列："
            + ", ".join(missing)
            + f"（表头为 {', '.join(c.strip() for c in header_raw)}）"
        )
    duplicates = {name for name in required if header.count(name) > 1}
    if duplicates:
        raise ValidationError("CSV 存在重复列：" + ", ".join(sorted(duplicates)))
    index = {name: header.index(name) for name in required}

    vehicle_ids: list[str] = []
    timestamps: list[datetime] = []
    latitudes: list[float] = []
    longitudes: list[float] = []

    for row_no, row in enumerate(reader, start=2):
        if not row or all(not cell.strip() for cell in row):
            continue  # tolerate blank separators, they are not observations
        if len(row) < len(header_raw):
            raise ValidationError(
                f"第 {row_no} 行：只有 {len(row)} 列，表头有 {len(header_raw)} 列"
            )
        if len(row) > len(header_raw):
            raise ValidationError(
                f"第 {row_no} 行：有 {len(row)} 列，多于表头的 {len(header_raw)} 列"
            )
        if len(timestamps) >= max_rows:
            raise ValidationError(
                f"单批最多 {max_rows} 行；第 {row_no} 行超出上限"
            )
        vehicle_id = row[index["vehicle_id"]].strip()
        if not vehicle_id:
            raise ValidationError(f"第 {row_no} 行：vehicle_id 为空")
        if len(vehicle_id) > 256:
            raise ValidationError(f"第 {row_no} 行：vehicle_id 超过 256 字符")
        vehicle_ids.append(vehicle_id)
        timestamps.append(_parse_timestamp(row[index["timestamp"]], row_no))
        latitudes.append(_parse_coordinate(row[index["latitude"]], "latitude", row_no, 90.0))
        longitudes.append(
            _parse_coordinate(row[index["longitude"]], "longitude", row_no, 180.0)
        )

    if not timestamps:
        raise ValidationError("CSV 没有数据行（仅表头）")

    arrays: Iterable[pa.Array] = [
        pa.array(vehicle_ids, type=pa.string()),
        pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
        pa.array(latitudes, type=pa.float64()),
        pa.array(longitudes, type=pa.float64()),
    ]
    try:
        table = pa.Table.from_arrays(list(arrays), schema=TRAJECTORY_SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        raise ValidationError(f"无法转换为 Parquet 表：{exc}") from None
    return ParsedBatch(table=table, row_count=len(timestamps))
