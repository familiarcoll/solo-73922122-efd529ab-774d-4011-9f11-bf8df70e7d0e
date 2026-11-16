from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app.main import create_app


def csv_bytes(count: int = 3, vehicle: str = "V1") -> bytes:
    lines = ["vehicle_id,timestamp,latitude,longitude"]
    lines += [
        f"{vehicle},2026-01-01T{index // 3600:02d}:{(index // 60) % 60:02d}:{index % 60:02d}Z,"
        f"{30.0 + index / 100},{120.5}"
        for index in range(count)
    ]
    return ("\n".join(lines) + "\n").encode()


@pytest.fixture()
def client(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv("LAKE_MAINTENANCE_INTERVAL_S", "3600")
    app = create_app(start_maintenance=False)
    app.state.registry.start()
    with TestClient(app) as test_client:
        yield test_client
    app.state.registry.stop()


def upload(client, batch_id: str, payload: bytes):
    return client.post(
        "/v1/batches",
        data={"batch_id": batch_id},
        files={"file": ("batch.csv", payload, "text/csv")},
    )


def test_docs_and_openapi_are_served(client) -> None:
    assert client.get("/docs").status_code == 200
    spec = client.get("/openapi.json").json()
    for path in (
        "/v1/batches",
        "/v1/files",
        "/v1/merges",
        "/v1/manifests",
        "/v1/manifests/{manifest_id}/release",
        "/v1/manifests/{manifest_id}/files/{file_id}/download",
    ):
        assert path in spec["paths"], path


def test_upload_claim_download_release(client) -> None:
    response = upload(client, "batch-1", csv_bytes(3))
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["file"]["rows"] == 3
    assert body["file"]["kind"] == "batch"

    manifest = client.post("/v1/manifests", json={"label": "job-1"}).json()
    assert manifest["state"] == "open"
    assert manifest["label"] == "job-1"
    assert manifest["row_count"] == 3
    item = manifest["files"][0]

    download = client.get(item["download_path"])
    assert download.status_code == 200
    assert download.headers["content-type"] == "application/vnd.apache.parquet"
    assert download.headers["x-file-sha256"] == item["sha256"]
    table = pq.read_table(pa.BufferReader(download.content))
    assert table.num_rows == 3
    assert table.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")

    released = client.post(
        f"/v1/manifests/{manifest['manifest_id']}/release"
    ).json()
    assert released["state"] == "released"
    assert released["already_released"] is False
    again = client.post(
        f"/v1/manifests/{manifest['manifest_id']}/release"
    ).json()
    assert again["already_released"] is True
    assert client.get(item["download_path"]).status_code == 410


def test_upload_validation(client) -> None:
    assert upload(client, "b-empty", b"").status_code == 400
    assert upload(client, "b-noheader", b"1,2,3,4\n").status_code == 400
    no_tz = b"vehicle_id,timestamp,latitude,longitude\nV1,2026-01-01T00:00:00,1,2\n"
    assert upload(client, "b-notz", no_tz).status_code == 400
    bad_lat = b"vehicle_id,timestamp,latitude,longitude\nV1,2026-01-01T00:00:00Z,91,2\n"
    assert upload(client, "b-lat", bad_lat).status_code == 400
    assert upload(client, "b-ok", csv_bytes(1000)).status_code == 201
    too_many = csv_bytes(1001)
    response = upload(client, "b-big", too_many)
    assert response.status_code == 400
    assert "1000" in response.json()["message"]


def test_duplicate_batch_and_unknown_ids(client) -> None:
    assert upload(client, "dup", csv_bytes(2)).status_code == 201
    duplicate = upload(client, "dup", csv_bytes(9))
    assert duplicate.status_code == 409
    assert client.get("/v1/manifests/mf_missing").status_code == 404
    assert client.post("/v1/manifests/mf_missing/release").status_code == 404
    manifest = client.post("/v1/manifests").json()
    assert (
        client.get(
            f"/v1/manifests/{manifest['manifest_id']}/files/b_nope/download"
        ).status_code
        == 404
    )


def test_merge_endpoint_matches_file_list(client) -> None:
    assert client.post("/v1/merges").status_code == 409  # nothing live yet
    for index in range(3):
        assert upload(client, f"m-{index}", csv_bytes(2, vehicle=f"V{index}")).status_code == 201
    merged = client.post("/v1/merges").json()
    assert merged["input_count"] == 3
    assert merged["row_count"] == 6
    assert merged["output_file"]["kind"] == "merged"
    live = client.get("/v1/files").json()
    assert [item["file_id"] for item in live["files"]] == [
        merged["output_file"]["file_id"]
    ]
    assert client.get("/v1/stats").json()["live_rows"] == 6
