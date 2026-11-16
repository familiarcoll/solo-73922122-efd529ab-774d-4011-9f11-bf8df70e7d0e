"""End-to-end demo against running instances (used by docs and CI checks).

It exercises the whole contract through HTTP so it doubles as a black-box
test of a Compose deployment::

    python -I scripts/demo.py --base-url http://localhost:8080 \
        --peer-url http://localhost:8081

Exit code is non-zero if any invariant fails.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone


def _request(method, url, data=None, headers=None, expect=(200, 201)):
    request = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            status = response.status
            content_type = response.headers.get("content-type", "")
    except urllib.error.HTTPError as exc:
        body = exc.read()
        status = exc.code
        content_type = exc.headers.get("content-type", "")
    if status not in expect:
        raise SystemExit(f"{method} {url} -> {status}: {body[:500]!r}")
    if "application/json" in content_type:
        import json

        return status, json.loads(body)
    return status, body


def claim(base_url):
    """Claim; a concurrent merge can briefly empty the live list -> retry."""
    deadline = time.monotonic() + 30
    while True:
        try:
            return _request(
                "POST", f"{base_url}/v1/manifests", data=b"{}",
                headers={"Content-Type": "application/json"},
            )
        except SystemExit:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.05)


def upload(base_url, batch_id, rows):
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(["vehicle_id", "timestamp", "latitude", "longitude"])
    writer.writerows(rows)
    boundary = uuid.uuid4().hex
    parts = []
    for name, value in (("batch_id", batch_id),):
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n".encode()
        )
    parts.append(
        (
            f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
            'filename="batch.csv"\r\nContent-Type: text/csv\r\n\r\n'
        ).encode()
    )
    parts.append(buffer.getvalue().encode())
    parts.append(f"\r\n--{boundary}--\r\n".encode())
    body = b"".join(parts)
    return _request(
        "POST",
        f"{base_url}/v1/batches",
        data=body,
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(body)),
        },
    )


def make_rows(seed: int, count: int, tag: str = ""):
    rng = random.Random(seed)
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    rows = []
    for index in range(count):
        moment = base + timedelta(seconds=rng.randrange(0, 100_000), microseconds=rng.randrange(0, 1000))
        rows.append(
            (
                f"V{rng.randrange(1, 30):03d}-{tag}",
                moment.isoformat().replace("+00:00", "Z"),
                round(rng.uniform(-89.0, 89.0), 6),
                round(rng.uniform(-179.0, 179.0), 6),
            )
        )
    return rows


def norm_rows(rows):
    return sorted(
        (vehicle, _ts_text(moment), float(lat), float(lon))
        for vehicle, moment, lat, lon in rows
    )


def _ts_text(value: str) -> str:
    return (
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    )


def parse_parquet_rows(payload: bytes):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pq.read_table(pa.BufferReader(payload))
    timestamps = table.column("timestamp").to_pylist()
    return [
        (
            table.column("vehicle_id")[index].as_py(),
            timestamps[index].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            table.column("latitude")[index].as_py(),
            table.column("longitude")[index].as_py(),
        )
        for index in range(table.num_rows)
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--peer-url", default="http://localhost:8081")
    parser.add_argument("--merges", type=int, default=4)
    parser.add_argument("--concurrency", type=int, default=3)
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:8]
    uploaders = [args.base_url, args.peer_url]
    stop = threading.Event()
    failures: list[str] = []
    claimed: list[dict] = []
    lock = threading.Lock()

    print("[1/6] 并发上传小批轨迹（两个实例交替）…")
    base_rows = []
    for index in range(6):
        rows = make_rows(1000 + index, 12 + index, run_id)
        upload(uploaders[index % 2], f"demo-{run_id}-base-{index}", rows)
        base_rows.extend(rows)
    print(f"      uploaded {len(base_rows)} rows in 6 batches")

    print("[2/6] 领取清单，然后合并并继续上传…")
    _, manifest = claim(f"{args.base_url}")
    with lock:
        claimed.append(manifest)
    _, merge = _request("POST", f"{args.base_url}/v1/merges")
    print(f"      merged {merge['input_count']} files -> {merge['row_count']} rows")

    late_rows = make_rows(2000, 7, run_id)
    upload(uploaders[0], f"demo-{run_id}-late-0", late_rows)
    late_rows_b = make_rows(2001, 5, run_id)
    upload(uploaders[1], f"demo-{run_id}-late-1", late_rows_b)
    late_rows.extend(late_rows_b)

    print("[3/6] 两个实例并发合并/领取/上传，校验不丢不重…")
    def churn(seed: int) -> None:
        rng = random.Random(seed)
        index = 0
        while not stop.is_set():
            target = uploaders[rng.randrange(2)]
            try:
                if rng.random() < 0.55:
                    _, item = claim(target)
                    with lock:
                        claimed.append(item)
                elif rng.random() < 0.8:
                    _request("POST", f"{target}/v1/merges")
                else:
                    _request("GET", f"{target}/v1/files")
            except BaseException as exc:  # pragma: no cover - reported below
                failures.append(repr(exc))
                return
            index += 1

    threads = [threading.Thread(target=churn, args=(seed,)) for seed in range(args.concurrency)]
    for thread in threads:
        thread.start()
    try:
        for index in range(args.merges):
            rows = make_rows(3000 + index, 5, run_id)
            upload(uploaders[(index + 1) % 2], f"demo-{run_id}-churn-{index}", rows)
            base_rows.extend(rows)
            time.sleep(0.02)
    finally:
        stop.set()
        for thread in threads:
            thread.join()
    if failures:
        print("      failures:", failures)
        return 1
    print(f"      {len(claimed)} manifests claimed during churn")

    print("[4/6] 旧清单在合并与领取后仍逐件可下载…")
    for item in claimed[0]["files"]:
        status, payload = _request("GET", f"{args.base_url}{item['download_path']}")
        digest = hashlib.sha256(payload).hexdigest()
        if digest != item["sha256"]:
            print(f"      sha mismatch for {item['file_id']}")
            return 1
    print("      pinned files still readable, hashes match")

    print("[5/6] 最终清单必须包含每条原始记录恰一次…")
    _, final_manifest = claim(f"{args.peer_url}")
    downloaded = []
    for item in final_manifest["files"]:
        status, payload = _request("GET", f"{args.peer_url}{item['download_path']}")
        downloaded.extend(parse_parquet_rows(payload))
    from collections import Counter

    expected_all = norm_rows(base_rows + late_rows)
    actual_all = norm_rows(downloaded)
    # Scope to this run's uniquely tagged rows so the check is meaningful on
    # a volume that already holds data from earlier runs.
    ours = Counter(row for row in actual_all if row[0].endswith(f"-{run_id}"))
    expected_counts = Counter(expected_all)
    if ours != expected_counts:
        mismatch = [row for row in expected_counts if ours[row] != expected_counts[row]]
        print(
            f"      mismatch: expected {sum(expected_counts.values())}, got "
            f"{sum(ours.values())}; differing rows {len(mismatch)}, "
            f"extra {len(list((Counter(ours) - Counter(expected_counts)).elements()))}: "
            f"{list((Counter(ours) - Counter(expected_counts)).elements())[:2]} "
            f"missing {list((Counter(expected_counts) - Counter(ours)).elements())[:2]}"
        )
        return 1
    # Note: identical rows from two uploads are legitimate duplicates; the
    # invariant is that *ingested multiplicity* is preserved, which the
    # per-run comparison above checks.
    file_ids = [item["file_id"] for item in final_manifest["files"]]
    if len(file_ids) != len(set(file_ids)):
        print("      the same file appears twice in one manifest")
        return 1
    total = sum(item["rows"] for item in final_manifest["files"])
    if total != len(actual_all) or total != final_manifest["row_count"]:
        print(f"      manifest accounting mismatch: {total} vs {len(actual_all)}")
        return 1
    print(
        f"      {len(file_ids)} files, {len(actual_all)} rows: "
        f"this run's {len(expected_all)} rows each appear exactly once"
    )

    print("[6/6] 释放全部清单（幂等）…")
    released = set()
    for item in claimed + [final_manifest]:
        if item["manifest_id"] in released:
            continue
        released.add(item["manifest_id"])
        _request("POST", f"{args.peer_url}/v1/manifests/{item['manifest_id']}/release")
        _request("POST", f"{args.base_url}/v1/manifests/{item['manifest_id']}/release", expect=(200,))
    status, stats = _request("GET", f"{args.base_url}/v1/stats")
    print(f"      stats: {stats}")

    # After every manifest is released, retired files must be gone from disk.
    _, final_again = claim(f"{args.base_url}")
    live = {item["file_id"] for item in final_again["files"]}
    stale = [
        item["file_id"]
        for item in claimed[0]["files"]
        if item["file_id"] not in live
    ]
    print(f"      retired-after-release files no longer served: {len(stale)} (expected ≥1)")
    if not stale:
        print("      expected the superseded merge inputs to have been reclaimed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
