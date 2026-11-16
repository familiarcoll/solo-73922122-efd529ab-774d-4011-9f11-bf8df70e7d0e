"""API request/response models (also used by the generated OpenAPI docs)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class FileInfo(BaseModel):
    file_id: str = Field(description="不可变 Parquet 文件 ID")
    seq: int = Field(description="登记顺序号，用于稳定的现行清单排序")
    kind: Literal["batch", "merged"] = Field(description="原始上传批次或合并结果")
    rows: int = Field(description="文件内数据行数（重复行按原样保留）")
    bytes: int = Field(description="Parquet 文件字节数")
    sha256: str = Field(description="Parquet 文件内容的 SHA-256")
    created_at: str = Field(description="UTC 登记时间（ISO 8601）")
    source_batch_id: str | None = Field(default=None, description="上传时使用的批号")
    merge_id: str | None = Field(default=None, description="生成该文件时的合并作业 ID")


class LiveFilesResponse(BaseModel):
    files: list[FileInfo]
    file_count: int
    row_count: int


class BatchUploadResponse(BaseModel):
    batch_id: str = Field(description="本次上传的批号")
    file: FileInfo
    message: str = "批次已登记为现行文件"


class MergeResponse(BaseModel):
    merge_id: str
    output_file: FileInfo
    input_file_ids: list[str] = Field(description="本次合并开始时快照到的现行文件")
    input_count: int
    row_count: int
    committed_at: str


class ManifestFile(BaseModel):
    ordinal: int
    file_id: str
    rows: int
    bytes: int
    sha256: str
    download_path: str = Field(description="相对 API 根路径的逐件下载地址")


class ManifestResponse(BaseModel):
    manifest_id: str
    label: str | None
    state: Literal["open", "released"]
    created_at: str
    released_at: str | None
    file_count: int
    row_count: int
    bytes: int
    files: list[ManifestFile]
    message: str | None = None


class ClaimRequest(BaseModel):
    label: str | None = Field(
        default=None,
        max_length=200,
        description="可选的作业标签，仅用于识别，不参与一致性判断",
    )


class ReleaseResponse(BaseModel):
    manifest_id: str
    state: Literal["released"]
    released_at: str
    already_released: bool
    message: str


class StatsResponse(BaseModel):
    live_files: int
    live_rows: int
    retired_files: int
    recycled_files: int
    open_manifests: int
    released_manifests: int
    total_files: int


class HealthResponse(BaseModel):
    status: str
    instance_id: str
    root: str
