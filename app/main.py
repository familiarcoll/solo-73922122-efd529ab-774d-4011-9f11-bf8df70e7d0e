"""FastAPI application exposing the trajectory data lake API.

Endpoints (all under ``/v1``):
  POST   /batches                         upload one CSV batch (<=1000 rows)
  GET    /files                           current live file list
  POST   /merges                          merge all live files into one
  POST   /manifests                       claim a fixed file list
  GET    /manifests/{manifest_id}         inspect a claimed list
  POST   /manifests/{manifest_id}/release release the claim
  GET    /manifests/{manifest_id}/files/{file_id}/download
  GET    /stats                           registry counters
  GET    /healthz                         liveness
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from .config import load_settings
from .errors import LakeError, ValidationError
from .models import (
    BatchUploadResponse,
    ClaimRequest,
    HealthResponse,
    LiveFilesResponse,
    ManifestFile,
    ManifestResponse,
    MergeResponse,
    ReleaseResponse,
    StatsResponse,
)
from .registry import Registry

DESCRIPTION = """
车载轨迹小批数据湖：每批 CSV 上传存为一件不可变 Parquet，重复行照原样保留；
合并只替换合并开始时的现行文件；分析作业领取固定文件清单，逐件下载后显式释放。
两个实例共享同一卷时，运行中清单的每条原始记录恰好出现一次。
"""


def create_app(*, start_maintenance: bool = True) -> FastAPI:
    settings = load_settings()
    registry = Registry(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_maintenance:
            registry.start()
        app.state.registry = registry
        yield
        if start_maintenance:
            registry.stop()

    app = FastAPI(
        title="Trajectory Data Lake",
        version="1.0.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    app.state.registry = registry

    @app.exception_handler(LakeError)
    async def lake_error_handler(_request: Request, exc: LakeError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.code, "message": exc.message},
        )

    @app.get("/healthz", response_model=HealthResponse, tags=["ops"])
    def healthz() -> HealthResponse:
        return HealthResponse(
            status="ok",
            instance_id=registry.instance_id,
            root=str(settings.root),
        )

    @app.get("/v1/stats", response_model=StatsResponse, tags=["ops"])
    def stats() -> StatsResponse:
        return StatsResponse(**registry.stats())

    @app.post(
        "/v1/batches",
        response_model=BatchUploadResponse,
        status_code=201,
        tags=["ingest"],
        summary="上传一批 CSV（至多 1000 行）",
    )
    async def upload_batch(
        batch_id: str = Form(..., description="批号，全局唯一"),
        file: UploadFile = File(..., description="车辆号,UTC时刻,纬度,经度 的 CSV"),
    ) -> BatchUploadResponse:
        payload = await file.read(settings.max_upload_bytes + 1)
        if len(payload) > settings.max_upload_bytes:
            raise ValidationError(
                f"上传内容超过 {settings.max_upload_bytes} 字节上限"
            )
        result = registry.ingest_batch(batch_id, payload)
        return BatchUploadResponse(
            batch_id=result["batch_id"],
            file=result["file"],
        )

    @app.get(
        "/v1/files",
        response_model=LiveFilesResponse,
        tags=["files"],
        summary="查看当前现行文件清单",
    )
    def list_files() -> LiveFilesResponse:
        return LiveFilesResponse(**registry.list_live())

    @app.post(
        "/v1/merges",
        response_model=MergeResponse,
        tags=["merge"],
        summary="把合并开始时的现行文件合并为一件",
    )
    def merge() -> MergeResponse:
        result = registry.merge()
        return MergeResponse(
            merge_id=result["merge_id"],
            output_file=registry.get_file(result["output_file_id"]),
            input_file_ids=result["input_file_ids"],
            input_count=result["input_count"],
            row_count=result["row_count"],
            committed_at=result["committed_at"],
        )

    @app.post(
        "/v1/manifests",
        response_model=ManifestResponse,
        status_code=201,
        tags=["manifests"],
        summary="领取一份固定文件清单",
    )
    def claim(payload: ClaimRequest | None = None) -> ManifestResponse:
        label = payload.label if payload else None
        return _manifest_response(registry.claim(label))

    @app.get(
        "/v1/manifests/{manifest_id}",
        response_model=ManifestResponse,
        tags=["manifests"],
        summary="查看已领取清单",
    )
    def get_manifest(manifest_id: str) -> ManifestResponse:
        return _manifest_response(registry.get_manifest(manifest_id))

    @app.post(
        "/v1/manifests/{manifest_id}/release",
        response_model=ReleaseResponse,
        tags=["manifests"],
        summary="显式释放清单",
    )
    def release(manifest_id: str) -> ReleaseResponse:
        return ReleaseResponse(**registry.release(manifest_id))

    @app.get(
        "/v1/manifests/{manifest_id}/files/{file_id}/download",
        tags=["manifests"],
        summary="按清单逐件下载 Parquet",
        response_class=StreamingResponse,
        responses={
            200: {
                "content": {"application/vnd.apache.parquet": {}},
                "description": "Parquet 文件字节流",
            },
            404: {"description": "清单或文件不存在"},
            410: {"description": "清单已经释放"},
        },
    )
    def download(manifest_id: str, file_id: str) -> StreamingResponse:
        handle, size, sha256, filename = registry.open_for_download(
            manifest_id, file_id
        )

        def body():
            try:
                with handle:
                    while chunk := handle.read(256 * 1024):
                        yield chunk
            finally:
                # Orphaned descriptors are eventually garbage collected, but
                # closing deterministically keeps the fd count bounded.
                if not handle.closed:
                    handle.close()

        return StreamingResponse(
            body(),
            media_type="application/vnd.apache.parquet",
            headers={
                "Content-Length": str(size),
                "X-File-Sha256": sha256,
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )

    return app


def _manifest_response(payload: dict) -> ManifestResponse:
    return ManifestResponse(
        manifest_id=payload["manifest_id"],
        label=payload["label"],
        state=payload["state"],
        created_at=payload["created_at"],
        released_at=payload["released_at"],
        file_count=payload["file_count"],
        row_count=payload["row_count"],
        bytes=payload["bytes"],
        files=[ManifestFile(**item) for item in payload["files"]],
        message=payload["message"],
    )


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.environ.get("LAKE_HOST", "0.0.0.0"),
        port=int(os.environ.get("LAKE_PORT", "8000")),
    )
