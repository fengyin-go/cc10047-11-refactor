"""存储优化/压缩恢复编排的服务层入口。

规范实现是函数式的 :mod:`app.storage_flow`（状态落在内存仓库）；
本模块一方面再导出这些函数，另一方面提供 ``StorageOptimizeService``
类形态，便于通过 ``service_for("storage_optimize")`` 或直接导入使用。
"""
from __future__ import annotations

from typing import Any

from app import storage_flow
from app.services.base import BaseModuleService
from app.storage_flow import *  # noqa: F401,F403
from app.storage_flow import (  # noqa: F401
    CURRENT,
    FROZEN,
    add_file,
    boundary_label,
    build_boundary_views,
    confirm_merge_with_size,
    ensure_merge_confirmed,
    get_history,
    get_todo,
    mark_interrupted,
    normalize_boundary,
    recover_compaction,
    refresh_todo,
    resume_compaction,
    run_reference_scenario,
    verify_handoff,
)


class StorageOptimizeService(BaseModuleService):
    """把合并交接、边界恢复和压缩待办收拢到一个服务入口。"""

    MODULE = "storage_optimize"
    ENTITY = "存储优化"
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}

    # —— 合并确认 / 历史 ——
    def domain_confirm_merge(self, merge_id: int, size_mib: Any = None):
        return storage_flow.confirm_merge_with_size(merge_id, size_mib)

    def domain_merge_history(self, merge_id: int):
        return storage_flow.get_history(merge_id)

    def domain_history(self, merge_id: int):
        return storage_flow.get_history(merge_id)

    # —— 文件目录 ——
    def domain_add_file(self, merge_id: int | None = 1, **kwargs: Any):
        return storage_flow.add_file(merge_id=merge_id, **kwargs)

    def domain_add_committed_file(self, merge_id: int | None = 1, **kwargs: Any):
        kwargs.setdefault("committed", True)
        return storage_flow.add_file(merge_id=merge_id, **kwargs)

    # —— 交接核验 ——
    def domain_verify_handoff(self, compaction_id: int, merge_id: int | None = None):
        return storage_flow.verify_handoff(compaction_id, merge_id)

    # —— 待办 ——
    def domain_todo(self, compaction_id: int, merge_id: int | None = None):
        return storage_flow.get_todo(compaction_id, merge_id)

    def domain_compaction_todo(self, compaction_id: int, merge_id: int | None = None):
        return storage_flow.get_todo(compaction_id, merge_id)

    # —— 中断 / 恢复 ——
    def domain_interrupt(self, compaction_id: int, reason: str | None = None):
        return storage_flow.mark_interrupted(compaction_id, reason)

    def domain_resume(self, compaction_id: int, boundary: Any = None,
                      merge_id: int | None = None, **opts: Any):
        return storage_flow.resume_compaction(
            compaction_id, boundary=boundary, merge_id=merge_id, **opts)

    domain_recover = domain_resume
    domain_retry = domain_resume

    def domain_reference_scenario(self, merge_id: int = 1,
                                  compaction_id: int = 1, **kwargs: Any):
        return storage_flow.run_reference_scenario(
            merge_id=merge_id, compaction_id=compaction_id, **kwargs)


# 类名近义别名
StorageFlowService = StorageOptimizeService
CompactionRecoveryService = StorageOptimizeService
StorageService = StorageOptimizeService
