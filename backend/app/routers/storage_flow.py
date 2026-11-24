"""存储优化恢复流程的专用 HTTP 入口。

通用资源动作 ``POST /api/{module}/{id}/actions`` 已支持中文场景动作；
本路由再提供一组语义化路径，方便按“合并交接 / 压缩待办 / 恢复重试”调用。
同一组处理函数会以多组路径别名挂载，降低不同调用约定之间的差异。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app import storage_flow
from app.services import service_for
from app.store import store

router = APIRouter(prefix="/api", tags=["存储优化恢复"])


async def _options(request: Request) -> dict[str, Any]:
    """合并 query 参数与 JSON body（body 缺省/非对象时不报错）。"""
    opts: dict[str, Any] = {k: v for k, v in request.query_params.items()}
    if request.method in ("POST", "PUT", "PATCH"):
        try:
            body = await request.json()
        except Exception:
            body = None
        if isinstance(body, dict):
            for key, value in body.items():
                if value is not None:
                    opts[key] = value
    return opts


def _as_int(value: Any, default: Any = None) -> Any:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ------------------------------------------------------------------
# 合并侧
# ------------------------------------------------------------------

@router.post("/storage-flow/merge/{merge_id}/confirm")
@router.post("/storage-optimize/merge/{merge_id}/confirm")
@router.post("/partition_merge/{merge_id}/confirm")
async def confirm_merge(merge_id: int, request: Request) -> dict[str, Any]:
    opts = await _options(request)
    svc = service_for("partition_merge")
    size = opts.get("size_mib", opts.get("size_mb"))
    return svc.domain_confirm_merge(merge_id, size_mib=size)


@router.post("/storage-flow/merge/{merge_id}/files")
@router.post("/storage-optimize/merge/{merge_id}/files")
@router.post("/partition_merge/{merge_id}/files")
async def add_merge_file(merge_id: int, request: Request) -> dict[str, Any]:
    opts = await _options(request)
    svc = service_for("partition_merge")
    return svc.domain_add_file(
        merge_id,
        size_mib=opts.get("size_mib", opts.get("size_mb", 16)),
        size_bytes=opts.get("size_bytes"),
        committed=_parse_bool(opts.get("committed"), True),
        half_written=_parse_bool(opts.get("half_written"), False),
        partial=_parse_bool(opts.get("partial"), False),
        path=opts.get("path"),
        name=opts.get("name"),
        file_id=opts.get("file_id"),
        status=opts.get("status"),
        origin=opts.get("origin"),
    )


@router.get("/storage-flow/merge/{merge_id}/history")
@router.get("/storage-optimize/merge/{merge_id}/history")
@router.get("/partition_merge/{merge_id}/history")
async def merge_history(merge_id: int) -> dict[str, Any]:
    return storage_flow.get_history(merge_id)


@router.get("/storage-flow/merge/{merge_id}/frozen-scope")
@router.get("/storage-optimize/merge/{merge_id}/frozen-scope")
async def frozen_scope(merge_id: int) -> dict[str, Any]:
    merge = storage_flow.ensure_merge_confirmed(merge_id)
    if merge is None:
        return {"ok": False, "message": f"合并任务 {merge_id} 不存在"}
    return {"ok": True, "merge_id": merge_id,
            "frozen_scope": merge.get("frozen_scope")}


async def _merge_resume(merge_id: int, request: Request) -> dict[str, Any]:
    opts = await _options(request)
    boundary = (opts.get("boundary") or opts.get("execution_boundary")
                or opts.get("mode") or opts.get("scope"))
    compaction_id = _as_int(opts.get("compaction_id"), merge_id) or merge_id
    return storage_flow.resume_compaction(
        compaction_id, boundary=boundary, merge_id=merge_id)


@router.post("/partition_merge/{merge_id}/resume")
@router.post("/partition_merge/{merge_id}/recover")
@router.post("/storage-flow/merge/{merge_id}/resume")
@router.post("/storage-flow/merge/{merge_id}/recover")
@router.post("/storage-optimize/merge/{merge_id}/resume")
async def merge_resume(merge_id: int, request: Request) -> dict[str, Any]:
    return await _merge_resume(merge_id, request)


# ------------------------------------------------------------------
# 压缩侧：待办 / 中断 / 恢复
# ------------------------------------------------------------------

@router.get("/storage-flow/compaction/{compaction_id}/todo")
@router.get("/storage-optimize/compaction/{compaction_id}/todo")
@router.get("/compaction_task/{compaction_id}/todo")
@router.get("/compaction/{compaction_id}/todo")
async def compaction_todo(compaction_id: int, merge_id: int | None = None) -> dict[str, Any]:
    return storage_flow.get_todo(compaction_id, merge_id)


@router.get("/storage-flow/compaction/{compaction_id}/history")
@router.get("/compaction_task/{compaction_id}/history")
async def compaction_history(compaction_id: int, merge_id: int | None = None) -> dict[str, Any]:
    comp = store.find("compaction_task", compaction_id)
    if comp is None:
        return {"ok": False, "message": f"压缩任务 {compaction_id} 不存在"}
    mid = merge_id or storage_flow.resolve_merge_id(comp)
    return storage_flow.get_history(int(mid or 1))


@router.post("/storage-flow/compaction/{compaction_id}/interrupt")
@router.post("/storage-optimize/compaction/{compaction_id}/interrupt")
@router.post("/compaction_task/{compaction_id}/interrupt")
async def interrupt_compaction(compaction_id: int, request: Request) -> dict[str, Any]:
    opts = await _options(request)
    return storage_flow.mark_interrupted(compaction_id, opts.get("reason"))


async def _resume(compaction_id: int, request: Request) -> dict[str, Any]:
    opts = await _options(request)
    boundary = (opts.get("boundary") or opts.get("execution_boundary")
                or opts.get("mode") or opts.get("scope"))
    merge_id = _as_int(opts.get("merge_id"))
    extra = {k: v for k, v in opts.items()
             if k not in ("boundary", "execution_boundary", "mode", "scope", "merge_id")}
    return storage_flow.resume_compaction(
        compaction_id, boundary=boundary, merge_id=merge_id, **extra)


@router.post("/storage-flow/compaction/{compaction_id}/resume")
@router.post("/storage-flow/compaction/{compaction_id}/recover")
@router.post("/storage-flow/compaction/{compaction_id}/retry")
@router.post("/storage-optimize/compaction/{compaction_id}/resume")
@router.post("/storage-optimize/compaction/{compaction_id}/recover")
@router.post("/compaction_task/{compaction_id}/resume")
@router.post("/compaction_task/{compaction_id}/recover")
@router.post("/compaction_task/{compaction_id}/retry")
@router.post("/compaction/{compaction_id}/resume")
@router.post("/compaction/{compaction_id}/recover")
@router.post("/compactions/{compaction_id}/resume")
async def resume_compaction(compaction_id: int, request: Request) -> dict[str, Any]:
    return await _resume(compaction_id, request)


@router.post("/compaction_task/{compaction_id}/files")
@router.post("/storage-flow/compaction/{compaction_id}/files")
async def add_compaction_file(compaction_id: int, request: Request) -> dict[str, Any]:
    opts = await _options(request)
    svc = service_for("compaction_task")
    return svc.domain_add_file(
        compaction_id,
        size_mib=opts.get("size_mib", opts.get("size_mb", 16)),
        size_bytes=opts.get("size_bytes"),
        committed=_parse_bool(opts.get("committed"), True),
        half_written=_parse_bool(opts.get("half_written"), False),
        path=opts.get("path"), name=opts.get("name"),
        file_id=opts.get("file_id"), merge_id=_as_int(opts.get("merge_id")))


@router.post("/storage-flow/verify-handoff/{compaction_id}")
@router.post("/storage-optimize/verify-handoff/{compaction_id}")
async def verify_handoff(compaction_id: int, request: Request) -> dict[str, Any]:
    opts = await _options(request)
    return storage_flow.verify_handoff(
        compaction_id, _as_int(opts.get("merge_id")))


# ------------------------------------------------------------------
# 一键剧本 / 链路别名
# ------------------------------------------------------------------

@router.post("/storage-flow/reference-scenario")
@router.post("/storage-optimize/reference-scenario")
@router.post("/chains/storage-optimize-recovery")
@router.post("/chains/compaction-recovery")
async def reference_scenario(request: Request) -> dict[str, Any]:
    opts = await _options(request)
    merge_id = _as_int(opts.get("merge_id"), 1) or 1
    compaction_id = _as_int(opts.get("compaction_id"), merge_id) or merge_id
    second_boundary = opts.get("second_boundary", storage_flow.CURRENT)
    if str(second_boundary).lower() in ("none", "null", "false", ""):
        second_boundary = None
    return storage_flow.run_reference_scenario(
        merge_id=merge_id, compaction_id=compaction_id,
        second_boundary=second_boundary)


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in ("0", "false", "no", "否", "未提交")
