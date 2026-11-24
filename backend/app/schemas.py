"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    action: str
    remark: str | None = None
    # 存储优化/压缩恢复场景的可选参数（全部宽松类型，缺省不影响既有动作）
    boundary: Any = None
    execution_boundary: Any = None
    mode: Any = None
    scope: Any = None
    merge_id: Any = None
    size_mib: Any = None
    size_mb: Any = None
    size_bytes: Any = None
    committed: Any = None
    half_written: Any = None
    partial: Any = None
    path: Any = None
    name: Any = None
    file_id: Any = None
    reason: Any = None
