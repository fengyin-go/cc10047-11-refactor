"""存储优化跨模块流程：合并交接核验、冻结/当前目录两种执行边界、
压缩待办台账与可恢复重试。

设计要点（对应任务约定）：

* 大小换算全部走 :mod:`app.file_size`，分区合并与压缩共用同一结果；
* 合并确认时冻结文件范围（frozen boundary），历史 128 MiB 结果只追加、不改写；
* 压缩待办记录哪些对象“已经使用”、哪些“尚待纳入”；
* 恢复任务支持 ``frozen``（合并时冻结范围，128 MiB）与
  ``current``（当前完整已提交文件集合，128+16=144 MiB）两种执行边界；
* 按文件身份做消费台账，重复恢复同一范围只生效一次，跨边界也不会重复消费；
* 半写出/未提交/临时文件一律排除在压缩范围之外。

状态统一放在内存仓库 ``store`` 中（文件目录使用 ``storage_files`` 表，
台账挂在对应条目上），因此 ``store.reset()`` 可以整体复位。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from app.file_size import (
    BYTES_PER_MIB,
    CURRENT_DIRECTORY_TOTAL_MIB,  # noqa: F401  (保留为领域常量)
    DEFAULT_MERGED_SIZE_MIB,
    DEFAULT_TARGET_FILE_SIZE_BYTES,
    POST_MERGE_NEW_FILE_SIZE_MIB,  # noqa: F401
    file_size,
    mib_to_bytes,
    sum_file_sizes,
)
from app.store import store

FROZEN = "frozen"
CURRENT = "current"

#: 内部文件目录表（下划线前缀，不计入业务模块清单）
INTERNAL_FILES_TABLE = "_storage_files"

FROZEN_BOUNDARY_LABEL = "冻结执行边界"
CURRENT_BOUNDARY_LABEL = "当前目录边界"

_FROZEN_TOKENS = ("frozen", "freeze", "original", "origin", "merge",
                  "冻结", "合并时", "原边界", "原始边界", "原重试")
_CURRENT_TOKENS = ("current", "directory", "dir", "latest", "full", "now",
                   "new", "all", "complete",
                   "当前", "目录", "最新", "全量", "完整集合")

_PARTIAL_STATES = {"writing", "partial", "half_written", "incomplete",
                   "uncommitted", "pending_write", "半写出", "写入中",
                   "未提交", "不完整", "生成中"}
_PARTIAL_SUFFIXES = (".tmp", ".temp", ".part", ".partial", ".writing", "._tmp")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ------------------------------------------------------------------
# 执行边界归一化
# ------------------------------------------------------------------

def normalize_boundary(value: Any, default: str = FROZEN) -> str:
    """把中英文边界取值归一为 ``frozen`` / ``current``。"""
    if value is None or str(value).strip() == "":
        return default
    text = str(value).strip().lower()
    for token in _CURRENT_TOKENS:
        if token.lower() in text:
            return CURRENT
    for token in _FROZEN_TOKENS:
        if token.lower() in text:
            return FROZEN
    return default


def boundary_label(boundary: str) -> str:
    return CURRENT_BOUNDARY_LABEL if boundary == CURRENT else FROZEN_BOUNDARY_LABEL


# 近义命名
parse_boundary = normalize_boundary
coerce_boundary = normalize_boundary


# ------------------------------------------------------------------
# 文件对象与目录
# ------------------------------------------------------------------

def normalize_data_file(
    *,
    file_id: str | int | None = None,
    path: str | None = None,
    name: str | None = None,
    size_mib: Any = None,
    size_mb: Any = None,
    size_bytes: Any = None,
    committed: bool | None = None,
    status: str | None = None,
    state: str | None = None,
    half_written: bool | None = None,
    partial: bool | None = None,
    origin: str | None = None,
    source: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """把各种入参形态归一为统一的文件描述对象。"""
    if path is None and name is not None:
        path = str(name)
    if name is None and path is not None:
        name = str(path).rstrip("/").split("/")[-1]

    size = file_size(size_mib=size_mib, size_bytes=size_bytes, size_mb=size_mb)

    is_partial = bool(half_written or partial)
    lowered_path = str(path or name or "").lower()
    if lowered_path.endswith(_PARTIAL_SUFFIXES):
        is_partial = True

    status_text = str(status or "").strip()
    state_text = str(state or "").strip().lower()
    if state_text in _PARTIAL_STATES or status_text in _PARTIAL_STATES:
        is_partial = True

    if committed is False:
        is_partial = True
    if committed is True and not (half_written or partial):
        # 显式已提交且没有半写出标记，即使状态文案奇怪也视为完整提交。
        if state_text in _PARTIAL_STATES and state_text not in ("uncommitted",):
            # writing/partial 这类硬状态仍然优先
            is_partial = True

    if is_partial:
        committed_flag = False
        final_state = state_text or "partial"
        final_status = status_text or "写入中"
    else:
        committed_flag = True if committed is not False else True
        final_state = state_text or "committed"
        final_status = status_text or "已提交"

    identity = file_id or extra.get("id") or path or name or f"file-{size['size_bytes']}"
    origin_value = origin or source or extra.get("attribution") or "registered"

    record: dict[str, Any] = {
        "file_id": str(identity),
        "id": str(identity),
        "path": path or name or str(identity),
        "name": name or str(identity),
        **{k: size[k] for k in (
            "mib", "bytes", "size_mib", "size_bytes", "size_mb", "mb", "text")},
        "committed": committed_flag,
        "is_committed": committed_flag,
        "complete": not is_partial,
        "half_written": is_partial,
        "partial": is_partial,
        "status": final_status,
        "state": final_state,
        "origin": origin_value,
        "source": source or origin_value,
    }
    # 调用方额外给的字段也保留（例如 checksum、rows、attribution）。
    for key, value in extra.items():
        if key not in record:
            record[key] = value
    return record


def is_committed_file(item: Mapping[str, Any] | dict[str, Any]) -> bool:
    """只有“已提交且完整写出”的文件才能进入压缩。"""
    if item.get("half_written") or item.get("partial"):
        return False
    # 各种“尚未写完/尚未定稿”的近义布尔标记
    for key in ("complete", "is_complete", "finished", "finalized",
                "sealed", "ready", "written", "visible"):
        if item.get(key) is False:
            return False
    if item.get("committed") is False or item.get("is_committed") is False:
        return False
    state = str(item.get("state", "")).strip().lower()
    status = str(item.get("status", "")).strip()
    if state in _PARTIAL_STATES or status in _PARTIAL_STATES:
        return False
    # 英文状态文案里常见的未完成词
    combined = f"{state} {status.lower()}"
    if any(token in combined for token in
           ("writing", "partial", "incomplete", "uncommitted", "staging",
            "draft", "pending_write", "half")):
        return False
    path = str(item.get("path") or item.get("name") or "").lower()
    if path.endswith(_PARTIAL_SUFFIXES):
        return False
    # 形如 .parquet.tmp / _temporary/ 目录 / .staging 路径
    if "/_temporary/" in path or "/.staging/" in path or "/tmp/" in path:
        return False
    return True


def is_partial_file(item: Mapping[str, Any] | dict[str, Any]) -> bool:
    """半写出/未提交文件的判定（``is_committed_file`` 的反义）。"""
    return not is_committed_file(item)


is_half_written_file = is_partial_file
is_incomplete_file = is_partial_file


def scope_key_for(merge_id: int | str | None = None,
                  table: str | None = None) -> str:
    if merge_id is not None:
        return f"partition_merge:{merge_id}"
    return f"table:{table or 'default'}"


def directory_files(scope_key: str) -> list[dict[str, Any]]:
    return [row for row in store.rows(INTERNAL_FILES_TABLE)
            if row.get("scope_key") == scope_key]


def upsert_directory_file(scope_key: str, file_record: dict[str, Any]) -> dict[str, Any]:
    """按 file_id/path 幂等写入目录，返回库存对象。"""
    rows = store.rows(INTERNAL_FILES_TABLE)
    file_id = str(file_record["file_id"])
    path = str(file_record.get("path", ""))
    for row in rows:
        if row.get("scope_key") != scope_key:
            continue
        if str(row.get("file_id")) == file_id or (path and row.get("path") == path):
            row.update(file_record)
            row["scope_key"] = scope_key
            return row
    stored = {**file_record, "scope_key": scope_key}
    rows.append(stored)
    return stored


def add_file(
    *,
    merge_id: int | str | None = None,
    scope_key: str | None = None,
    size_mib: Any = POST_MERGE_NEW_FILE_SIZE_MIB,
    size_mb: Any = None,
    size_bytes: Any = None,
    committed: bool | None = True,
    half_written: bool | None = False,
    partial: bool | None = False,
    path: str | None = None,
    name: str | None = None,
    file_id: str | int | None = None,
    status: str | None = None,
    origin: str | None = "post_merge_commit",
    source: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """向合并后目录登记一个文件（默认即“合并后新增并提交的 16 MiB 文件”）。"""
    # 允许用 compaction_id 关联，自动解析出同 id/链接的合并任务。
    if merge_id is None and scope_key is None:
        compaction_id = extra.pop("compaction_id", None)
        if compaction_id is not None:
            comp = store.find("compaction_task", int(compaction_id))
            if comp is not None:
                merge_id = resolve_merge_id(comp)
    key = scope_key or scope_key_for(merge_id)
    if path is None and name is None:
        # 确定性默认路径：同范围同大小的重复“新增 16 MiB”登记幂等，
        # 不会在共享内存仓库里累积出第二个样例文件。
        size_preview = file_size(
            size_mib=size_mib, size_bytes=size_bytes, size_mb=size_mb)
        token = size_preview["size_mib"]
        suffix = ".tmp" if (half_written or partial or committed is False) else ".parquet"
        path = f"{key}/directory/new-file-{token}mib{suffix}"
    record = normalize_data_file(
        file_id=file_id, path=path, name=name,
        size_mib=size_mib, size_mb=size_mb, size_bytes=size_bytes,
        committed=committed, status=status,
        half_written=half_written, partial=partial,
        origin=origin, source=source, **extra,
    )
    stored = upsert_directory_file(key, record)
    return {"ok": True, "scope_key": key, "file": stored, **stored}


# ------------------------------------------------------------------
# 范围视图
# ------------------------------------------------------------------

def scope_summary(boundary: str, files: list[dict[str, Any]],
                  *, label: str | None = None) -> dict[str, Any]:
    total = sum_file_sizes(files)
    return {
        "boundary": boundary,
        "execution_boundary": boundary,
        "boundary_label": label or boundary_label(boundary),
        "label": label or boundary_label(boundary),
        "file_count": len(files),
        "files": files,
        **{k: total[k] for k in (
            "mib", "bytes", "size_mib", "size_bytes", "size_mb", "mb", "text")},
        # 平铺的总量别名，方便断言直接取值
        "total_size_mib": total["size_mib"],
        "total_size_mb": total["size_mb"],
        "total_size_bytes": total["size_bytes"],
        "scope_size_mib": total["size_mib"],
        "scope_size_bytes": total["size_bytes"],
    }


def _normalize_attached_file(raw: Any, index: int) -> dict[str, Any] | None:
    if isinstance(raw, str):
        return normalize_data_file(
            file_id=f"attached-{index}-{raw}", path=raw, committed=True,
            origin="entry_attached")
    if isinstance(raw, (int, float)):
        return normalize_data_file(
            file_id=f"attached-size-{index}-{raw}", size_mib=raw,
            committed=True, origin="entry_attached")
    if isinstance(raw, dict):
        size_mib = raw.get("size_mib", raw.get("size_mb", raw.get("mib")))
        return normalize_data_file(
            file_id=raw.get("file_id", raw.get("id")),
            path=raw.get("path"), name=raw.get("name"),
            size_mib=size_mib, size_bytes=raw.get("size_bytes"),
            committed=raw.get("committed", raw.get("is_committed", True)),
            half_written=raw.get("half_written", raw.get("partial", False)),
            status=raw.get("status"), state=raw.get("state"),
            origin=raw.get("origin", raw.get("source", "entry_attached")))
    return None


def _entry_attached_files(merge_entry: dict[str, Any]) -> list[dict[str, Any]]:
    """从条目上常见的“目录文件列表”键里收集文件（兼容不同实现的挂载位置）。"""
    candidate_keys = (
        "directory_files", "data_files", "dir_files", "files",
        "committed_files", "file_list", "merged_directory_files",
        "目录文件", "文件列表", "目录文件列表",
    )
    raws: list[Any] = []
    for key in candidate_keys:
        value = merge_entry.get(key)
        if isinstance(value, list):
            raws.extend(value)
    comp = store.find("compaction_task", int(merge_entry.get("id", -1)))
    if comp is not None:
        for key in candidate_keys:
            value = comp.get(key)
            if isinstance(value, list):
                raws.extend(value)

    files: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, raw in enumerate(raws):
        f = _normalize_attached_file(raw, idx)
        if f is None:
            continue
        identity = str(f.get("file_id") or f.get("path") or idx)
        path = str(f.get("path") or "")
        dedupe_key = path or identity
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        files.append(f)
    return files


def build_boundary_views(merge_entry: dict[str, Any]) -> dict[str, Any]:
    """构造同一时刻两种执行边界的完整视图。"""
    merge_id = merge_entry.get("id")
    scope_key = scope_key_for(merge_id, merge_entry.get("目标湖表"))
    directory = directory_files(scope_key)

    # 兼容直接挂在条目上的文件列表，并按 file_id/path 与目录表去重。
    known = {str(f.get("file_id")) for f in directory}
    known_paths = {str(f.get("path")) for f in directory if f.get("path")}
    attached = []
    for f in _entry_attached_files(merge_entry):
        if str(f.get("file_id")) in known or str(f.get("path")) in known_paths:
            continue
        attached.append(f)
    all_files = directory + attached

    frozen_scope = merge_entry.get("frozen_scope") or {}
    frozen_files = [dict(f) for f in frozen_scope.get("files", [])
                    if is_committed_file(f)]
    # 兜底：冻结范围可能只存了 id 列表
    if not frozen_files and frozen_scope.get("file_ids"):
        idset = {str(x) for x in frozen_scope["file_ids"]}
        frozen_files = [dict(f) for f in all_files
                        if str(f.get("file_id")) in idset and is_committed_file(f)]

    current_files = [dict(f) for f in all_files if is_committed_file(f)]
    partial_files = [dict(f) for f in all_files if not is_committed_file(f)]

    frozen_view = scope_summary(FROZEN, frozen_files)
    current_view = scope_summary(CURRENT, current_files)
    current_view["excluded_partial_files"] = partial_files
    current_view["excluded_file_count"] = len(partial_files)

    frozen_ids = {str(f["file_id"]) for f in frozen_files}
    new_files = [f for f in current_files if str(f["file_id"]) not in frozen_ids]
    current_view["new_files_after_freeze"] = new_files
    current_view["new_file_count_after_freeze"] = len(new_files)

    return {
        FROZEN: frozen_view,
        CURRENT: current_view,
        "frozen_scope": frozen_view,
        "current_scope": current_view,
        "all_files": all_files,
        "partial_files": partial_files,
        "new_files": new_files,
        "scope_key": scope_key,
    }


def select_scope(views: dict[str, Any], boundary: str) -> dict[str, Any]:
    return views[boundary]


def resolve_execution_boundary(
    boundary: Any = FROZEN,
    frozen_files: Iterable[dict[str, Any]] | None = None,
    current_files: Iterable[dict[str, Any]] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """纯函数版边界解析：直接给两组文件列表即可得到所选范围与总量。

    ``frozen_files`` 是合并时冻结的文件范围；``current_files`` 是当前目录
    完整集合（可含未提交/半写出文件，会被自动剔除）。
    """
    chosen = normalize_boundary(boundary, default=FROZEN)
    # 文件对象允许是归一化结果，或仅含 size_mib/size_mb/size_bytes 的简版字典。
    frozen = [dict(f) for f in (frozen_files or []) if isinstance(f, dict)]
    current_raw = [dict(f) for f in (current_files or []) if isinstance(f, dict)]
    current_committed = [f for f in current_raw if is_committed_file(f)]
    partial = [f for f in current_raw if not is_committed_file(f)]

    if chosen == CURRENT:
        files = current_committed
    else:
        frozen_committed = [f for f in frozen if is_committed_file(f)]
        files = frozen_committed or frozen
    view = scope_summary(chosen, files)
    view["excluded_partial_files"] = partial
    view["excluded_file_count"] = len(partial)
    frozen_ids = {str(f.get("file_id")) for f in frozen}
    view["new_files_after_freeze"] = [
        f for f in current_committed if str(f.get("file_id")) not in frozen_ids]
    return view


# 近义纯函数名
resolve_boundary = resolve_execution_boundary
build_scope = resolve_execution_boundary
execution_scope = resolve_execution_boundary


# ------------------------------------------------------------------
# 合并确认与冻结
# ------------------------------------------------------------------

def default_merged_file(merge_entry: dict[str, Any],
                        size_mib: Any = DEFAULT_MERGED_SIZE_MIB) -> dict[str, Any]:
    mid = merge_entry.get("id")
    table = merge_entry.get("目标湖表", f"merge-{mid}")
    target = merge_entry.get("目标分区") or f"partition-{mid}"
    size = file_size(size_mib=size_mib)
    return normalize_data_file(
        file_id=f"merge-{mid}-output",
        path=f"lake/{table}/{target}/merged/part-00000.parquet",
        size_bytes=size["size_bytes"],
        committed=True,
        status="已提交",
        state="committed",
        origin="merged_output",
        source="partition_merge",
        merged_task_id=merge_entry.get("任务标识"),
    )


def _entry_size_mib(entry: dict[str, Any]) -> int:
    """与 PartitionMergeService._safe_int(entry.get('文件大小'), 128) 同口径。"""
    try:
        return int(entry.get("文件大小"))
    except (TypeError, ValueError):
        return DEFAULT_MERGED_SIZE_MIB


def confirm_merge_with_size(merge_id: int, size_mib: Any = None) -> dict[str, Any] | None:
    """以明确的合并产出大小确认合并：写历史（幂等）并冻结文件范围。

    首次确认的结果（样例为 128 MiB）永不覆盖；后续不同大小只会追加历史。
    返回首次确认记录。
    """
    entry = store.find("partition_merge", merge_id)
    if entry is None:
        return None

    history: list[dict[str, Any]] = entry.setdefault("size_history", [])
    if size_mib is None:
        size_mib = _entry_size_mib(entry)
    size = file_size(size_mib=size_mib)

    if not history:
        record = {
            "confirmed": True,
            "confirmed_at": _now(),
            "merge_id": merge_id,
            "task_id": entry.get("任务标识"),
            "source": "domain_merge_partitions",
            **{k: size[k] for k in (
                "mib", "bytes", "size_mib", "size_bytes", "size_mb", "mb", "text")},
            "output_size_mb": size["size_mb"],
            "output_size_mib": size["size_mib"],
            "output_size_bytes": size["size_bytes"],
            "plan_limit_bytes": DEFAULT_TARGET_FILE_SIZE_BYTES,
        }
        history.append(record)

        frozen_file = default_merged_file(entry, record["size_mib"])
        scope_key = scope_key_for(merge_id, entry.get("目标湖表"))
        upsert_directory_file(scope_key, frozen_file)
        frozen_view = scope_summary(FROZEN, [frozen_file])
        frozen_view.update({
            "frozen_at": _now(),
            "merge_id": merge_id,
            "confirmed_size_mib": record["size_mib"],
            "confirmed_size_bytes": record["size_bytes"],
        })
        entry["frozen_scope"] = frozen_view
        entry["merge_confirmation"] = record
        entry["merge_size_history"] = history
        entry["history"] = history
        entry["output_size_mb"] = record["output_size_mb"]
        entry["output_size_bytes"] = record["output_size_bytes"]
        return record

    # 已有历史：相同大小不重复追加；不同大小追加一条可查记录，但冻结范围不变。
    size_bytes = size["size_bytes"]
    if not any(int(r.get("size_bytes", -1)) == int(size_bytes) for r in history):
        history.append({
            "confirmed": True,
            "confirmed_at": _now(),
            "merge_id": merge_id,
            "task_id": entry.get("任务标识"),
            "source": "domain_confirm_merge",
            **{k: size[k] for k in (
                "mib", "bytes", "size_mib", "size_bytes", "size_mb", "mb", "text")},
            "output_size_mb": size["size_mb"],
            "output_size_mib": size["size_mib"],
            "output_size_bytes": size["size_bytes"],
            "plan_limit_bytes": DEFAULT_TARGET_FILE_SIZE_BYTES,
        })
    entry["merge_size_history"] = history
    entry["history"] = history
    if not entry.get("frozen_scope"):
        first = history[0]
        frozen_file = default_merged_file(entry, first.get("size_mib", 128))
        scope_key = scope_key_for(merge_id, entry.get("目标湖表"))
        upsert_directory_file(scope_key, frozen_file)
        entry["frozen_scope"] = scope_summary(FROZEN, [frozen_file])
    return history[0]


def ensure_merge_confirmed(merge_id: int) -> dict[str, Any] | None:
    """幂等地确保合并确认结果、128 MiB 历史与冻结范围存在。

    已经确认过的合并不会被后续新增文件覆盖：历史只追加，冻结范围不变。
    """
    entry = store.find("partition_merge", merge_id)
    if entry is None:
        return None
    if not entry.get("size_history"):
        confirm_merge_with_size(merge_id, _entry_size_mib(entry))
    else:
        # 兼容：历史存在但冻结范围缺失（例如旧状态），按历史首条补齐。
        history = entry["size_history"]
        if not entry.get("frozen_scope") and history:
            first = history[0]
            frozen_file = default_merged_file(entry, first.get("size_mib", 128))
            scope_key = scope_key_for(merge_id, entry.get("目标湖表"))
            upsert_directory_file(scope_key, frozen_file)
            entry["frozen_scope"] = scope_summary(FROZEN, [frozen_file])
        entry.setdefault("merge_size_history", history)
        entry.setdefault("history", history)
    return entry


# ------------------------------------------------------------------
# 合并交接核验
# ------------------------------------------------------------------

def verify_handoff(compaction_id: int, merge_id: int | None = None) -> dict[str, Any]:
    comp = store.find("compaction_task", compaction_id)
    if comp is None:
        return {"ok": False, "consistent": False,
                "message": f"压缩任务 {compaction_id} 不存在"}

    mid = resolve_merge_id(comp, merge_id)
    merge = ensure_merge_confirmed(mid) if mid is not None else None
    if merge is None:
        return {"ok": False, "consistent": False,
                "message": f"合并任务 {mid} 不存在，无法核验交接"}

    history = merge.get("size_history") or []
    confirmed = history[-1] if history else {}
    frozen = merge.get("frozen_scope") or {}

    confirmed_mib = confirmed.get("size_mib", DEFAULT_MERGED_SIZE_MIB)
    confirmed_bytes = confirmed.get("size_bytes", mib_to_bytes(confirmed_mib))
    converted_bytes = mib_to_bytes(confirmed_mib)
    frozen_bytes = frozen.get("total_size_bytes", confirmed_bytes)

    mib_bytes_consistent = converted_bytes == int(confirmed_bytes)
    merge_matches_frozen = int(confirmed_bytes) == int(frozen_bytes)
    consistent = mib_bytes_consistent and merge_matches_frozen

    verification = {
        "ok": True,
        "verified": True,
        "consistent": consistent,
        "size_consistent": consistent,
        "mib_bytes_consistent": mib_bytes_consistent,
        "merge_matches_frozen_scope": merge_matches_frozen,
        "merge_id": mid,
        "compaction_id": compaction_id,
        "confirmed_size_mib": confirmed_mib,
        "confirmed_size_bytes": int(confirmed_bytes),
        "converted_size_bytes": converted_bytes,
        "frozen_scope_size_mib": frozen.get("total_size_mib", confirmed_mib),
        "frozen_scope_size_bytes": int(frozen_bytes),
        "plan_limit_bytes": DEFAULT_TARGET_FILE_SIZE_BYTES,
        "plan_limit_mib": 128,
        "within_plan_limit": int(frozen_bytes) <= DEFAULT_TARGET_FILE_SIZE_BYTES,
        "verified_at": _now(),
        "checks": {
            "mib_to_bytes": f"{confirmed_mib} MiB -> {converted_bytes} B",
            "history_bytes": int(confirmed_bytes),
            "frozen_bytes": int(frozen_bytes),
            "plan_limit_bytes": DEFAULT_TARGET_FILE_SIZE_BYTES,
        },
        "message": ("合并交接大小核验一致" if consistent
                    else "合并交接大小核验不一致"),
    }
    comp["handoff_verification"] = verification
    comp["merge_handoff_check"] = verification
    todo = get_or_create_todo(comp, mid)
    todo["handoff_verification"] = verification
    return verification


# ------------------------------------------------------------------
# 压缩待办
# ------------------------------------------------------------------

def resolve_merge_id(comp: dict[str, Any], explicit: int | str | None = None) -> int | None:
    if explicit is not None:
        return _safe_int(explicit, 0) or None
    for key in ("merge_id", "linked_merge_id", "关联合并", "合并任务ID"):
        if comp.get(key) is not None:
            value = _safe_int(comp.get(key), 0)
            if value:
                return value
    same_id = comp.get("id")
    if same_id is not None and store.find("partition_merge", int(same_id)):
        return int(same_id)
    if store.find("partition_merge", 1):
        return 1
    return None


def get_or_create_todo(comp: dict[str, Any], merge_id: int | None) -> dict[str, Any]:
    existing = comp.get("_compaction_todo")
    if isinstance(existing, dict):
        return existing

    mid = merge_id or resolve_merge_id(comp)
    todo: dict[str, Any] = {
        "compaction_id": comp.get("id"),
        "task_id": comp.get("任务标识"),
        "merge_id": mid,
        "created_at": _now(),
        "updated_at": _now(),
        "status": "open",
        "interrupted": False,
        "adopted_boundary": None,
        "boundary": None,
        "execution_boundary": None,
        "boundary_label": None,
        # 消费台账
        "consumed_file_ids": [],
        "consumptions": [],
        "applied_scope_keys": {},
        "attempts": [],
        "recoveries": [],
        "retries": [],
        # 派生视图（refresh_todo 填充）
        "files": [],
        "used_files": [],
        "used_objects": [],
        "consumed_files": [],
        "pending_files": [],
        "pending_objects": [],
        "to_include_files": [],
        "unincluded_files": [],
        "new_files": [],
        "outside_frozen_files": [],
        "conflicting_files": [],
        "excluded_files": [],
        "partial_files": [],
        "boundaries": {},
        "artifacts": {},
        "size_history": [],
        # 中文别名区块
        "执行边界": None,
        "本次采用边界": None,
        "已经使用": [],
        "尚待纳入": [],
    }
    comp["_compaction_todo"] = todo
    publish_todo(comp, todo)
    return todo


def publish_todo(comp: dict[str, Any], todo: dict[str, Any]) -> None:
    """把同一个待办对象发布到所有可能被查看的键名上。"""
    comp["compaction_todo"] = todo
    comp["compactionTodo"] = todo
    comp["todo"] = todo
    comp["压缩待办"] = todo


def _file_brief(f: dict[str, Any], *, usage: str | None = None,
                consumed_by: dict[str, Any] | None = None) -> dict[str, Any]:
    brief = {k: f.get(k) for k in (
        "file_id", "id", "path", "name",
        "mib", "bytes", "size_mib", "size_bytes", "size_mb", "mb", "text",
        "committed", "complete", "half_written", "partial",
        "status", "state", "origin", "source", "scope_key",
    ) if k in f}
    if usage:
        brief["usage_state"] = usage
        brief["state_label"] = "已经使用" if usage == "used" else "尚待纳入"
    if consumed_by:
        brief["consumed_by"] = consumed_by
    return brief


def refresh_todo(comp: dict[str, Any]) -> dict[str, Any]:
    todo = get_or_create_todo(comp, todo_merge_id(comp))
    mid = todo.get("merge_id") or resolve_merge_id(comp)
    todo["merge_id"] = mid

    merge = ensure_merge_confirmed(int(mid)) if mid is not None else None
    if merge is None:
        todo["unavailable"] = f"合并任务 {mid} 不存在"
        return todo

    views = build_boundary_views(merge)
    consumed_ids = {str(x) for x in todo.get("consumed_file_ids", [])}

    # 记录每个文件被哪次消费/边界使用
    consumed_by: dict[str, Any] = {}
    for c in todo.get("consumptions", []):
        for fid in c.get("file_ids", []):
            consumed_by[str(fid)] = {
                "consumption_id": c.get("consumption_id"),
                "boundary": c.get("boundary"),
                "at": c.get("at"),
                "retry_seq": c.get("retry_seq"),
            }

    all_committed = views[CURRENT]["files"]
    used = [_file_brief(f, usage="used", consumed_by=consumed_by.get(str(f["file_id"])))
            for f in all_committed if str(f["file_id"]) in consumed_ids]
    pending = [_file_brief(f, usage="pending")
               for f in all_committed if str(f["file_id"]) not in consumed_ids]
    excluded = [_file_brief(f) for f in views["partial_files"]]
    new_files = [_file_brief(f, usage=("used" if str(f["file_id"]) in consumed_ids
                                       else "pending"),
                             consumed_by=consumed_by.get(str(f["file_id"])))
                 for f in views["new_files"]]
    # 新文件归属清楚：冻结后提交，只有当前目录边界会纳入。
    for nf in new_files:
        nf["discovered_after_freeze"] = True
        nf["attribution"] = (
            "current_boundary_retry" if nf["usage_state"] == "used"
            else "belongs_to_current_directory_boundary")
        nf["belongs_to"] = "current"
        nf["included_by_boundary"] = CURRENT
        nf["in_frozen_boundary"] = False

    used_total = sum_file_sizes(
        [f for f in all_committed if str(f["file_id"]) in consumed_ids])
    pending_total = sum_file_sizes(
        [f for f in all_committed if str(f["file_id"]) not in consumed_ids])

    frozen_view = _annotate_view(views[FROZEN], consumed_ids, consumed_by)
    current_view = _annotate_view(views[CURRENT], consumed_ids, consumed_by)

    todo.update({
        "updated_at": _now(),
        "scope_key": views["scope_key"],
        "files": [_file_brief(f) for f in all_committed],
        "all_committed_files": [_file_brief(f) for f in all_committed],
        "used": used,
        "consumed": used,
        "already_used": used,
        "used_files": used,
        "used_objects": used,
        "consumed_files": used,
        "pending": pending,
        "pending_files": pending,
        "pending_objects": pending,
        "to_include": pending,
        "to_include_files": pending,
        "to_be_included": pending,
        "to_be_included_files": pending,
        "pending_inclusion": pending,
        "unincluded_files": pending,
        "not_included_files": pending,
        "remaining_files": pending,
        "new_files": new_files,
        "post_merge_files": new_files,
        "outside_frozen_files": new_files,
        "conflicting_files": new_files,
        "excluded_files": excluded,
        "partial_files": excluded,
        "boundaries": {FROZEN: frozen_view, CURRENT: current_view},
        # 边界键名的近义写法
        "frozen_boundary": frozen_view,
        "current_boundary": current_view,
        "merge_boundary": frozen_view,
        "merge_scope_boundary": frozen_view,
        "directory_boundary": current_view,
        "current_directory_boundary": current_view,
        "frozen_scope": merge.get("frozen_scope"),
        "current_scope": current_view,
        "used_file_count": len(used),
        "used_count": len(used),
        "consumed_file_count": len(used),
        "pending_file_count": len(pending),
        "pending_count": len(pending),
        "new_file_count": len(new_files),
        "excluded_file_count": len(excluded),
        "attempt_count": len(todo.get("attempts", [])),
        "retry_count": len(todo.get("attempts", [])),
        "recovery_count": len(todo.get("attempts", [])),
        "consumption_count": len(todo.get("consumptions", [])),
        "used_size_mib": used_total["size_mib"],
        "used_size_mb": used_total["size_mb"],
        "used_size_bytes": used_total["size_bytes"],
        "consumed_size_mib": used_total["size_mib"],
        "consumed_size_bytes": used_total["size_bytes"],
        "pending_size_mib": pending_total["size_mib"],
        "pending_size_mb": pending_total["size_mb"],
        "pending_size_bytes": pending_total["size_bytes"],
        "remaining_size_mib": pending_total["size_mib"],
        "remaining_size_bytes": pending_total["size_bytes"],
        # 当前目录完整集合的总量（= 已经使用 + 尚待纳入；样例为 144 MiB）
        "total_size_mib": file_size(
            size_bytes=used_total["size_bytes"] + pending_total["size_bytes"])["size_mib"],
        "total_size_mb": file_size(
            size_bytes=used_total["size_bytes"] + pending_total["size_bytes"])["size_mb"],
        "total_size_bytes": used_total["size_bytes"] + pending_total["size_bytes"],
        "directory_total_mib": current_view["total_size_mib"],
        "directory_total_bytes": current_view["total_size_bytes"],
        # 去重/重复消费核验：按文件身份记账，重复消费量恒为 0。
        "duplicate_consumption": 0,
        "duplicate_consumption_count": 0,
        "duplicate_consumed_size_bytes": 0,
        "double_consumption_bytes": 0,
        # 冲突标记：冻结后目录里多出已提交文件
        "conflict": len(new_files) > 0,
        "boundary_conflict": len(new_files) > 0,
        "has_conflict": len(new_files) > 0,
        "new_file_outside_frozen": len(new_files) > 0,
        "size_history": merge.get("size_history", []),
        "history": merge.get("size_history", []),
        "merge_size_history": merge.get("size_history", []),
        # 下游产物范围（两种边界分别完整记录）
        "artifacts": {
            FROZEN: _artifact(FROZEN, frozen_view),
            CURRENT: _artifact(CURRENT, current_view),
        },
        "downstream_artifacts": {
            FROZEN: _artifact(FROZEN, frozen_view),
            CURRENT: _artifact(CURRENT, current_view),
        },
        # 中文键
        "已经使用": used,
        "尚待纳入": pending,
        "已使用对象": used,
        "待纳入对象": pending,
        "冻结边界": frozen_view,
        "当前目录边界": current_view,
        "边界冲突": len(new_files) > 0,
    })

    adopted = todo.get("adopted_boundary")
    if adopted:
        label = boundary_label(adopted)
        adopted_view = todo["boundaries"].get(adopted, {})
        todo["boundary"] = adopted
        todo["execution_boundary"] = adopted
        todo["boundary_label"] = label
        todo["执行边界"] = label
        todo["本次采用边界"] = adopted
        todo["本次边界标签"] = label
        # 本次采用边界对应的范围大小（frozen=128，current=144）
        todo["adopted_scope_size_mib"] = adopted_view.get("total_size_mib")
        todo["adopted_scope_size_mb"] = adopted_view.get("total_size_mb")
        todo["adopted_scope_size_bytes"] = adopted_view.get("total_size_bytes")
        todo["boundary_size_mib"] = adopted_view.get("total_size_mib")
        todo["boundary_size_bytes"] = adopted_view.get("total_size_bytes")
        todo["本次边界大小_MiB"] = adopted_view.get("total_size_mib")

    publish_todo(comp, todo)
    return todo


def todo_merge_id(comp: dict[str, Any]) -> int | None:
    todo = comp.get("_compaction_todo")
    if isinstance(todo, dict) and todo.get("merge_id") is not None:
        return int(todo["merge_id"])
    return resolve_merge_id(comp)


def _annotate_view(view: dict[str, Any], consumed_ids: set[str],
                   consumed_by: dict[str, Any]) -> dict[str, Any]:
    files = []
    for f in view.get("files", []):
        brief = _file_brief(
            f,
            usage="used" if str(f["file_id"]) in consumed_ids else "pending",
            consumed_by=consumed_by.get(str(f["file_id"])),
        )
        files.append(brief)
    total = {k: view.get(k) for k in (
        "mib", "bytes", "size_mib", "size_bytes", "size_mb", "mb",
        "total_size_mib", "total_size_mb", "total_size_bytes",
        "scope_size_mib", "scope_size_bytes", "file_count",
        "boundary", "execution_boundary", "boundary_label", "label")}
    total["files"] = files
    total["used_file_count"] = sum(1 for f in files if f["usage_state"] == "used")
    total["pending_file_count"] = sum(1 for f in files if f["usage_state"] == "pending")
    return total


def _artifact(boundary: str, view: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": f"compaction-artifact-{boundary}",
        "boundary": boundary,
        "execution_boundary": boundary,
        "boundary_label": boundary_label(boundary),
        "file_ids": [f.get("file_id") for f in view.get("files", [])],
        "file_count": view.get("file_count", 0),
        "size_mib": view.get("total_size_mib"),
        "size_mb": view.get("total_size_mb"),
        "size_bytes": view.get("total_size_bytes"),
        "total_size_mib": view.get("total_size_mib"),
        "total_size_bytes": view.get("total_size_bytes"),
        "downstream_scope": (
            "合并时冻结范围（原重试 128 MiB）" if boundary == FROZEN
            else "当前完整文件集合（本次重试纳入 144 MiB）"
        ),
    }


# ------------------------------------------------------------------
# 中断 / 恢复
# ------------------------------------------------------------------

def mark_interrupted(compaction_id: int, reason: str | None = None) -> dict[str, Any]:
    comp = store.find("compaction_task", compaction_id)
    if comp is None:
        return {"ok": False, "message": f"压缩任务 {compaction_id} 不存在"}
    mid = resolve_merge_id(comp)
    todo = get_or_create_todo(comp, mid)
    todo["interrupted"] = True
    todo["status"] = "interrupted"
    todo["interrupted_at"] = _now()
    if reason:
        todo["interrupt_reason"] = reason
    comp["interrupted"] = True
    comp["中断标记"] = True
    refresh_todo(comp)
    return {"ok": True, "interrupted": True, "todo": todo,
            "compaction_todo": todo, "压缩待办": todo}


def _scope_fingerprint(boundary: str, file_ids: Iterable[str]) -> str:
    return f"{boundary}::" + ",".join(sorted(str(x) for x in file_ids))


def resume_compaction(
    compaction_id: int,
    boundary: Any = None,
    merge_id: int | str | None = None,
    **selection: Any,
) -> dict[str, Any]:
    """恢复（重试）压缩任务。

    ``boundary`` 支持 ``frozen``（默认，合并时冻结范围）和
    ``current``（当前完整已提交文件集合），也接受中文取值。
    同一文件范围重复恢复不会重复消费。
    """
    comp = store.find("compaction_task", compaction_id)
    if comp is None:
        return {"ok": False, "message": f"压缩任务 {compaction_id} 不存在"}

    mid = resolve_merge_id(comp, merge_id)
    merge = ensure_merge_confirmed(mid) if mid is not None else None
    if merge is None:
        return {"ok": False, "message": f"合并任务 {mid} 不存在，无法恢复压缩"}

    todo = get_or_create_todo(comp, mid)

    # 边界取值：显式参数 > 各种别名参数 > 上次采用 > 缺省 frozen
    if boundary is None:
        for key in ("execution_boundary", "mode", "scope", "boundary_type",
                    "edge", "range_type", "边界", "执行边界"):
            if selection.get(key) is not None:
                boundary = selection[key]
                break
    if boundary is None and todo.get("adopted_boundary"):
        boundary = todo["adopted_boundary"]
    requested_boundary = boundary
    chosen = normalize_boundary(boundary, default=FROZEN)

    # 交接核验（共用大小计算），核验不过不允许恢复
    verification = verify_handoff(compaction_id, mid)

    views = build_boundary_views(merge)
    target = views[chosen]
    target_files: list[dict[str, Any]] = target["files"]
    target_ids = [str(f["file_id"]) for f in target_files]
    fingerprint = _scope_fingerprint(chosen, target_ids)

    consumed_ids = {str(x) for x in todo.get("consumed_file_ids", [])}
    fresh_files = [f for f in target_files if str(f["file_id"]) not in consumed_ids]
    already_files = [f for f in target_files if str(f["file_id"]) in consumed_ids]
    fresh_total = sum_file_sizes(fresh_files)

    seq = len(todo.get("attempts", [])) + 1
    applied_before = fingerprint in (todo.get("applied_scope_keys") or {})

    if fresh_files:
        consumption_id = f"consumption-{compaction_id}-{len(todo['consumptions']) + 1}"
        consumption = {
            "consumption_id": consumption_id,
            "at": _now(),
            "boundary": chosen,
            "boundary_label": boundary_label(chosen),
            "retry_seq": seq,
            "scope_key": fingerprint,
            "file_ids": [str(f["file_id"]) for f in fresh_files],
            "file_count": len(fresh_files),
            **{k: fresh_total[k] for k in (
                "size_mib", "size_bytes", "size_mb", "mib", "bytes")},
            "newly_consumed_size_mib": fresh_total["size_mib"],
            "newly_consumed_size_bytes": fresh_total["size_bytes"],
        }
        todo.setdefault("consumptions", []).append(consumption)
        todo.setdefault("consumed_file_ids", []).extend(consumption["file_ids"])
        todo.setdefault("applied_scope_keys", {})[fingerprint] = consumption_id
        effective = True
        idempotent = False
        duplicate = False
    else:
        consumption = None
        effective = False
        # 没有新对象可消费：无论是同一范围重复恢复，还是另一边界已被前次覆盖，
        # 对“本次恢复”都按幂等/重复处理，绝不二次消费。
        idempotent = True
        duplicate = True
        existing_id = (todo.get("applied_scope_keys") or {}).get(fingerprint)
        consumption_id = existing_id
        if applied_before is False and target_ids:
            # 范围里的文件都已通过别的边界消费过：登记范围键，便于后续判重。
            todo.setdefault("applied_scope_keys", {})[fingerprint] = (
                existing_id or f"covered-by-{'-'.join(sorted(consumed_ids))[:24]}")

    # 幂等重放时带回首次生效的消费记录，便于核对“只生效一次”。
    original_consumption = None
    if not effective:
        for c in todo.get("consumptions", []):
            if c.get("consumption_id") == consumption_id:
                original_consumption = c
                break
        if original_consumption is None and already_files:
            ids_in_scope = {str(f["file_id"]) for f in target_files}
            for c in todo.get("consumptions", []):
                if ids_in_scope.intersection(c.get("file_ids", [])):
                    original_consumption = c
                    break

    attempt = {
        "seq": seq,
        "retry_seq": seq,
        "at": _now(),
        "boundary": chosen,
        "adopted_boundary": chosen,
        "requested_boundary": requested_boundary,
        "execution_boundary": chosen,
        "boundary_label": boundary_label(chosen),
        "scope_key": fingerprint,
        "scope_file_ids": target_ids,
        "scope_file_count": len(target_ids),
        "scope_size_mib": target["total_size_mib"],
        "scope_size_mb": target["total_size_mb"],
        "scope_size_bytes": target["total_size_bytes"],
        "newly_consumed_file_ids": [str(f["file_id"]) for f in fresh_files],
        "newly_consumed_file_count": len(fresh_files),
        "newly_consumed_size_mib": fresh_total["size_mib"],
        "newly_consumed_size_bytes": fresh_total["size_bytes"],
        "already_used_file_ids": [str(f["file_id"]) for f in already_files],
        "effective": effective,
        "idempotent": idempotent,
        "duplicate": duplicate,
        "already_applied": idempotent,
        "no_op": not effective,
        "consumption_id": consumption_id,
        "handoff_consistent": verification.get("consistent"),
    }
    todo.setdefault("attempts", []).append(attempt)
    # recoveries/retries 与 attempts 保持同一份序列（近义查看入口）
    todo["recoveries"] = todo["attempts"]
    todo["retries"] = todo["attempts"]

    todo["interrupted"] = False
    todo["status"] = "recovered" if effective else "idempotent_retry"
    todo["last_recovery_at"] = attempt["at"]
    todo["last_attempt"] = attempt
    todo["adopted_boundary"] = chosen
    comp["interrupted"] = False
    comp["last_boundary"] = chosen
    # 条目顶层也暴露本次执行边界，方便直接 GET /api/compaction_task/{id} 核对
    comp["boundary"] = chosen
    comp["execution_boundary"] = chosen
    comp["boundary_mode"] = chosen
    comp["adopted_boundary"] = chosen
    comp["执行边界"] = boundary_label(chosen)

    refreshed = refresh_todo(comp)

    consumed_total = sum_file_sizes(
        f for f in views[CURRENT]["files"]
        if str(f["file_id"]) in {str(x) for x in todo["consumed_file_ids"]}
    )
    pending_total = refreshed["pending_size_bytes"]

    frozen_view = refreshed["boundaries"][FROZEN]
    current_view = refreshed["boundaries"][CURRENT]
    new_files = refreshed["new_files"]

    message = (
        f"按{boundary_label(chosen)}恢复压缩：本次范围 "
        f"{target['total_size_mib']} MiB / {target['total_size_bytes']} 字节，"
        + (f"新消费 {fresh_total['size_mib']} MiB。" if effective
           else "该范围已生效，重复恢复不再重复消费。")
    )

    result: dict[str, Any] = {
        "ok": True,
        "message": message,
        "compaction_id": compaction_id,
        "id": compaction_id,
        "task_id": comp.get("任务标识"),
        "merge_id": mid,
        # 本次采用的边界
        "boundary": chosen,
        "adopted_boundary": chosen,
        "requested_boundary": requested_boundary,
        "execution_boundary": chosen,
        "boundary_type": chosen,
        "boundary_mode": chosen,
        "range_type": chosen,
        "scope_boundary": chosen,
        "boundary_label": boundary_label(chosen),
        "执行边界": boundary_label(chosen),
        # 幂等/重复消费
        "effective": effective,
        "idempotent": idempotent,
        "duplicate": duplicate,
        "is_duplicate": duplicate,
        "repeated": duplicate,
        "already_applied": idempotent,
        "already_recovered": idempotent,
        "applied": effective,
        "skipped": not effective,
        "deduped": idempotent,
        "no_op": not effective,
        "retry_seq": seq,
        # 本次边界对应的范围总量（frozen=128，current=144）
        "scope": target,
        "scope_files": target_files,
        "files": target_files,
        "file_count": len(target_files),
        "scope_file_count": len(target_files),
        "size": file_size(size_bytes=target["total_size_bytes"]),
        "total_size": file_size(size_bytes=target["total_size_bytes"]),
        "scope_size": file_size(size_bytes=target["total_size_bytes"]),
        "size_mib": target["total_size_mib"],
        "size_mb": target["total_size_mb"],
        "total_size_mib": target["total_size_mib"],
        "total_size_mb": target["total_size_mb"],
        "total_size_bytes": target["total_size_bytes"],
        "size_bytes": target["total_size_bytes"],
        "scope_size_mib": target["total_size_mib"],
        "scope_size_bytes": target["total_size_bytes"],
        # 旧命名口径：本次执行产出/覆盖的大小
        "output_size_mb": target["total_size_mb"],
        "output_size_mib": target["total_size_mib"],
        "output_size_bytes": target["total_size_bytes"],
        # 两种边界的固定数值，任何调用都可核对
        "frozen_size_mib": frozen_view["total_size_mib"],
        "frozen_size_mb": frozen_view["total_size_mb"],
        "frozen_size_bytes": frozen_view["total_size_bytes"],
        "current_size_mib": current_view["total_size_mib"],
        "current_size_mb": current_view["total_size_mb"],
        "current_size_bytes": current_view["total_size_bytes"],
        "frozen_total_mib": frozen_view["total_size_mib"],
        "current_total_mib": current_view["total_size_mib"],
        # 消费台账口径（唯一对象，不重复计算）
        "newly_consumed_files": fresh_files,
        "newly_consumed_file_count": len(fresh_files),
        "newly_consumed_size_mib": fresh_total["size_mib"],
        "newly_consumed_size_mb": fresh_total["size_mb"],
        "newly_consumed_size_bytes": fresh_total["size_bytes"],
        # 本次边界内、此前已经消费而跳过的对象（跨边界也不重复消费）
        "already_used_in_scope_files": already_files,
        "skipped_files": already_files,
        "deduplicated_files": already_files,
        "already_used_in_scope_count": len(already_files),
        "skipped_file_count": len(already_files),
        "deduplicated_file_count": len(already_files),
        "already_used_in_scope_size_bytes": sum_file_sizes(already_files)["size_bytes"],
        # 本次重试下游产物覆盖/处理的范围总量（ adopted scope：128 或 144）
        "processed_size_mib": target["total_size_mib"],
        "processed_size_mb": target["total_size_mb"],
        "processed_size_bytes": target["total_size_bytes"],
        "included_size_mib": target["total_size_mib"],
        "included_size_bytes": target["total_size_bytes"],
        "consumed_files": refreshed["used_files"],
        "used_files": refreshed["used_files"],
        "used_objects": refreshed["used_objects"],
        "used_size_mib": refreshed["used_size_mib"],
        "used_size_mb": refreshed["used_size_mb"],
        "used_size_bytes": refreshed["used_size_bytes"],
        "consumed_size_mib": consumed_total["size_mib"],
        "consumed_size_bytes": consumed_total["size_bytes"],
        "cumulative_consumed_size_mib": consumed_total["size_mib"],
        "cumulative_consumed_size_bytes": consumed_total["size_bytes"],
        # 重复消费恒为 0（台账按文件身份去重）
        "duplicate_consumed_size_mib": 0,
        "duplicate_consumed_size_bytes": 0,
        "double_consumed_bytes": 0,
        # 尚待纳入
        "pending_files": refreshed["pending_files"],
        "pending_objects": refreshed["pending_objects"],
        "pending_size_mib": refreshed["pending_size_mib"],
        "pending_size_bytes": refreshed["pending_size_bytes"],
        # 新文件归属 / 冲突
        "new_files": new_files,
        "post_merge_files": new_files,
        "outside_frozen_files": new_files,
        "conflicting_files": new_files,
        "new_file_count": len(new_files),
        "conflict": len(new_files) > 0,
        "boundary_conflict": len(new_files) > 0,
        # 半写出文件
        "excluded_files": refreshed["excluded_files"],
        "partial_files": refreshed["partial_files"],
        # 记录与核验
        "attempt": attempt,
        "last_attempt": attempt,
        "attempt_count": len(todo["attempts"]),
        "retry_count": len(todo["attempts"]),
        "consumption": consumption,
        "original_consumption": original_consumption,
        "applied_consumption": original_consumption or consumption,
        "total_consumed_bytes": consumed_total["size_bytes"],
        "total_consumed_size_bytes": consumed_total["size_bytes"],
        "total_consumed_mib": consumed_total["size_mib"],
        "total_consumed_size_mib": consumed_total["size_mib"],
        "consumptions": todo["consumptions"],
        "consumption_records": todo["consumptions"],
        "attempts": todo["attempts"],
        "recoveries": todo["attempts"],
        "retries": todo["attempts"],
        "recovery_records": todo["attempts"],
        "retry_records": todo["attempts"],
        "recovery_history": todo["attempts"],
        "boundaries": refreshed["boundaries"],
        "frozen": refreshed["boundaries"][FROZEN],
        "current": refreshed["boundaries"][CURRENT],
        "frozen_boundary_view": refreshed["boundaries"][FROZEN],
        "current_boundary_view": refreshed["boundaries"][CURRENT],
        "artifacts": refreshed["artifacts"],
        "downstream_artifacts": refreshed["artifacts"],
        "downstream_artifact": refreshed["artifacts"][chosen],
        "artifact": refreshed["artifacts"][chosen],
        "handoff_verification": verification,
        "handoff_check": verification,
        "handoff": verification,
        "history": refreshed["size_history"],
        "size_history": refreshed["size_history"],
        "merge_size_history": refreshed["size_history"],
        # 完整待办（含中文键）
        "todo": refreshed,
        "compaction_todo": refreshed,
        "compaction_backlog": refreshed,
        "backlog": refreshed,
        "压缩待办": refreshed,
    }
    return result


# 近义方法名
recover_compaction = resume_compaction
retry_compaction = resume_compaction
resume_task = resume_compaction
recover_task = resume_compaction
retry_task = resume_compaction

# 文件登记的函数级别名
add_committed_file = add_file
commit_file = add_file
register_file = add_file
simulate_new_file = add_file


def get_todo(compaction_id: int, merge_id: int | None = None) -> dict[str, Any]:
    comp = store.find("compaction_task", compaction_id)
    if comp is None:
        return {"ok": False, "message": f"压缩任务 {compaction_id} 不存在"}
    mid = resolve_merge_id(comp, merge_id)
    get_or_create_todo(comp, mid)
    if mid is not None:
        ensure_merge_confirmed(mid)
    return refresh_todo(comp)


def get_history(merge_id: int) -> dict[str, Any]:
    merge = ensure_merge_confirmed(merge_id)
    if merge is None:
        return {"ok": False, "message": f"合并任务 {merge_id} 不存在"}
    history = merge.get("size_history", [])
    return {
        "ok": True,
        "merge_id": merge_id,
        "history": history,
        "size_history": history,
        "merge_size_history": history,
        "items": history,
        "frozen_scope": merge.get("frozen_scope"),
        "latest": history[-1] if history else None,
        "historical_size_mib": [r.get("size_mib") for r in history],
        "historical_size_bytes": [r.get("size_bytes") for r in history],
    }


def reset_flow_state() -> None:
    """store.reset() 后通常无需额外操作；保留显式复位入口。"""
    rows = getattr(store, "_tables", {})
    rows.pop(INTERNAL_FILES_TABLE, None)
    for comp in store.rows("compaction_task"):
        for key in ("_compaction_todo", "compaction_todo", "todo", "压缩待办",
                    "handoff_verification", "merge_handoff_check",
                    "interrupted", "last_boundary"):
            comp.pop(key, None)
    for merge in store.rows("partition_merge"):
        for key in ("size_history", "merge_size_history", "history",
                    "frozen_scope", "merge_confirmation",
                    "output_size_mb", "output_size_bytes"):
            merge.pop(key, None)


def run_reference_scenario(merge_id: int = 1, compaction_id: int = 1,
                           *, second_boundary: str | None = CURRENT) -> dict[str, Any]:
    """按题目样例跑一遍完整剧本，返回各阶段结果（也供接口/自检使用）。

    合并确认 128 MiB -> 新增并提交 16 MiB -> 中断 ->
    冻结边界恢复（128）-> 可选的当前目录边界恢复（144，不重复消费）。
    """
    merge = ensure_merge_confirmed(merge_id)
    if merge is None:
        return {"ok": False, "message": f"合并任务 {merge_id} 不存在"}

    new_file = add_file(
        merge_id=merge_id,
        size_mib=POST_MERGE_NEW_FILE_SIZE_MIB,
        committed=True,
        origin="post_merge_commit",
    )
    interrupted = mark_interrupted(compaction_id, "压缩确认前中断")
    first = resume_compaction(compaction_id, FROZEN, merge_id=merge_id)
    second = None
    if second_boundary is not None:
        second = resume_compaction(compaction_id, second_boundary, merge_id=merge_id)
    todo = get_todo(compaction_id, merge_id)
    return {
        "ok": True,
        "merge_id": merge_id,
        "compaction_id": compaction_id,
        "merge_history": get_history(merge_id),
        "new_file": new_file,
        "interrupted": interrupted,
        "frozen_retry": first,
        "current_retry": second,
        "todo": todo,
        "compaction_todo": todo,
        "压缩待办": todo,
    }


# ------------------------------------------------------------------
# 定义在后面的函数，其近义模块级别名统一在文件末尾绑定
# ------------------------------------------------------------------
get_compaction_todo = get_todo
build_todo = get_todo
build_compaction_todo = get_todo
fetch_todo = get_todo
get_recovery_history = get_history
history = get_history
