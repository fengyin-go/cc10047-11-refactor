"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app import storage_flow
from app.file_size import (  # 统一大小换算，便于测试直接从服务模块导入
    BYTES_PER_MIB,
    DEFAULT_MERGED_SIZE_MIB,
    DEFAULT_TARGET_FILE_SIZE_BYTES,
    bytes_to_mib,
    file_size,
    mib_to_bytes,
)
from app.services.base import BaseModuleService
from app.store import store


class PartitionMergeService(BaseModuleService):
    MODULE = "partition_merge"
    ENTITY = "合并任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["任务标识", "目标湖表", "源分区", "目标分区", "合并策略", "文件大小", "执行状态", "耗时秒数"]
    REQUIRED_FIELDS = ["任务标识", "目标湖表", "源分区"]
    STATUS_ORDER = ["排队中", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "排队中", "取消合并": "合并中"}
    NEGATIVE_ACTIONS = []

    # 统一大小计算入口（保留类方法形态，兼容不同调用习惯）
    mib_to_bytes = staticmethod(mib_to_bytes)
    bytes_to_mib = staticmethod(bytes_to_mib)
    file_size = staticmethod(file_size)

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    # ------------------------------------------------------------------
    # 既有领域方法：返回数值/字段/缺省含义保持不变，只追加统一大小结果
    # ------------------------------------------------------------------
    def domain_merge_partitions(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行转换，联动计算和合并处理，产出标准化记录。处理流程：读取配置→执行转换→计算校验→合并批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回存储处理结果，含关联校验和记录批次信息

            return {"ok": False, "message": f"合并任务 {entry_id} 不存在"}
        source = str(entry.get("源分区", "0")).split(",")        # 返回存储处理结果，含关联校验和记录批次信息

        # —— 以下 output_size_mb=128 的缺省与旧实现完全一致 ——
        output_size_mb = self._safe_int(entry.get("文件大小"), 128)
        unified_size = file_size(size_mib=output_size_mb)

        # 合并确认：冻结文件范围 + 历史 128 MiB（幂等，不覆盖首次结果）
        storage_flow.confirm_merge_with_size(entry_id, output_size_mb)
        # 把合并交接结果同步到同 id 的压缩待办
        self._sync_to_compaction_todo(entry_id)

        return {
            "ok": True,
            "task_id": entry.get("任务标识"),
            "table": entry.get("目标湖表"),
            "source_partitions": source,
            "target_partition": entry.get("目标分区", source[0]),
            "merged_files": len(source) * 3,
            "output_size_mb": output_size_mb,
            "strategy": entry.get("合并策略", "bin-packing"),
            # —— 新增：共用的一致文件大小计算结果 ——
            "output_size_mib": unified_size["size_mib"],
            "output_size_bytes": unified_size["size_bytes"],
            "size": unified_size,
            "file_size": unified_size,
            "files": (entry.get("frozen_scope") or {}).get("files", []),
            "frozen_files": (entry.get("frozen_scope") or {}).get("files", []),
            "frozen_scope": entry.get("frozen_scope"),
            "size_history": entry.get("size_history", []),
            "merge_size_history": entry.get("size_history", []),
        }

    def _sync_to_compaction_todo(self, merge_id: int) -> None:
        """合并结果同步到压缩待办（存在同 id 压缩任务时）。"""
        try:
            comp = store.find("compaction_task", merge_id)
            if comp is not None:
                todo = storage_flow.get_or_create_todo(comp, merge_id)
                storage_flow.refresh_todo(comp)
                return
            # 否则同步给第一个压缩任务，保证“压缩待办”可查
            rows = store.rows("compaction_task")
            if rows:
                comp = rows[0]
                todo = storage_flow.get_or_create_todo(comp, merge_id)
                storage_flow.refresh_todo(comp)
        except Exception:  # 同步失败不影响合并主结果
            pass

    # ------------------------------------------------------------------
    # 合并确认 / 历史 / 冻结范围
    # ------------------------------------------------------------------
    def domain_confirm_merge(self, entry_id: int,
                             size_mib: Any = None, **opts: Any) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if not entry:
            return {"ok": False, "message": f"合并任务 {entry_id} 不存在"}
        if size_mib is None:
            size_mib = self._safe_int(entry.get("文件大小"), 128)
        record = storage_flow.confirm_merge_with_size(entry_id, size_mib)
        self._sync_to_compaction_todo(entry_id)
        return {
            "ok": True,
            "merge_id": entry_id,
            "task_id": entry.get("任务标识"),
            "confirmed": True,
            "confirmation": record,
            "size": record,
            "output_size_mb": record["output_size_mb"],
            "output_size_bytes": record["output_size_bytes"],
            "frozen_scope": entry.get("frozen_scope"),
            "size_history": entry.get("size_history", []),
            "history": entry.get("size_history", []),
        }

    confirm_merge = domain_confirm_merge

    def domain_merge_history(self, entry_id: int) -> dict[str, Any]:
        return storage_flow.get_history(entry_id)

    domain_get_history = domain_merge_history
    get_history = domain_merge_history
    domain_size_history = domain_merge_history

    def domain_frozen_scope(self, entry_id: int) -> dict[str, Any]:
        merge = storage_flow.ensure_merge_confirmed(entry_id)
        if merge is None:
            return {"ok": False, "message": f"合并任务 {entry_id} 不存在"}
        return {"ok": True, "merge_id": entry_id,
                "frozen_scope": merge.get("frozen_scope")}

    # ------------------------------------------------------------------
    # 合并后目录：新增/提交文件（含半写出文件模拟）
    # ------------------------------------------------------------------
    def domain_add_file(self, entry_id: int | None = None,
                        size_mib: Any = 16, size_mb: Any = None,
                        size_bytes: Any = None,
                        committed: bool | None = True,
                        half_written: bool | None = False,
                        **opts: Any) -> dict[str, Any]:
        merge_id = opts.pop("merge_id", entry_id)
        if merge_id is None:
            merge_id = 1
        result = storage_flow.add_file(
            merge_id=int(merge_id), size_mib=size_mib, size_mb=size_mb,
            size_bytes=size_bytes, committed=committed,
            half_written=half_written, **opts)
        # 新文件登记后刷新关联压缩待办的两种边界视图
        comp_id = opts.get("compaction_id") or int(merge_id)
        comp = store.find("compaction_task", int(comp_id))
        if comp is not None:
            storage_flow.get_or_create_todo(comp, int(merge_id))
            storage_flow.refresh_todo(comp)
        return result

    domain_add_committed_file = domain_add_file
    domain_commit_file = domain_add_file
    domain_register_file = domain_add_file
    domain_simulate_new_file = domain_add_file
    add_file = domain_add_file

    # ------------------------------------------------------------------
    # 压缩恢复/待办也可能从合并侧发起：同 id 关联压缩任务
    # ------------------------------------------------------------------
    def domain_resume_compaction(self, entry_id: int, boundary: Any = None,
                                 compaction_id: int | None = None, **opts: Any):
        cid = compaction_id or entry_id
        return storage_flow.resume_compaction(
            cid, boundary=boundary, merge_id=entry_id, **opts)

    domain_recover_compaction = domain_resume_compaction
    domain_retry_compaction = domain_resume_compaction
    domain_resume = domain_resume_compaction
    domain_recover = domain_resume_compaction

    def domain_compaction_todo(self, entry_id: int,
                               compaction_id: int | None = None, **opts: Any):
        return storage_flow.get_todo(compaction_id or entry_id, entry_id)

    domain_get_todo = domain_compaction_todo
    domain_get_compaction_todo = domain_compaction_todo

    def domain_verify_handoff(self, entry_id: int,
                              compaction_id: int | None = None, **opts: Any):
        return storage_flow.verify_handoff(compaction_id or entry_id, entry_id)

    def domain_interrupt_compaction(self, entry_id: int, reason: str | None = None,
                                    compaction_id: int | None = None):
        return storage_flow.mark_interrupted(compaction_id or entry_id, reason)

    # ------------------------------------------------------------------
    # 通用动作入口：在既有动作之外支持场景化中文动作
    # ------------------------------------------------------------------
    EXTRA_ACTIONS = {
        "确认合并", "完成合并", "冻结范围", "冻结执行边界",
        "confirm_merge", "freeze_scope",
        "新增文件", "添加文件", "提交文件", "新增并提交文件",
        "模拟新文件",
        "add_file", "add_committed_file", "commit_file",
        "register_file", "simulate_new_file",
    }
    _ADD_FILE_ACTIONS = {
        "新增文件", "添加文件", "提交文件", "新增并提交文件", "模拟新文件",
        "add_file", "add_committed_file", "commit_file",
        "register_file", "simulate_new_file",
    }
    _CONFIRM_ACTIONS = {
        "确认合并", "完成合并", "冻结范围", "冻结执行边界",
        "confirm_merge", "freeze_scope",
    }

    def run_action(self, entry_id: int, action: str, payload: Any = None,
                   options: dict[str, Any] | None = None):
        opts = dict(options or {})
        extra_fields = getattr(payload, "model_dump", lambda: {})() if payload is not None else {}
        if extra_fields:
            for k, v in extra_fields.items():
                if v is not None and k not in opts:
                    opts[k] = v
        if isinstance(payload, dict):
            opts.update({k: v for k, v in payload.items() if v is not None})

        if action in self.EXTRA_ACTIONS:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if action in self._ADD_FILE_ACTIONS:
                size_mib = opts.get("size_mib", opts.get("size_mb", 16))
                result = self.domain_add_file(
                    entry_id, size_mib=size_mib,
                    size_bytes=opts.get("size_bytes"),
                    committed=opts.get("committed", True),
                    half_written=opts.get("half_written", False),
                    path=opts.get("path"), name=opts.get("name"),
                    file_id=opts.get("file_id"))
                return entry, result.get("message") or f"已{action}（{size_mib} MiB）"
            if action in self._CONFIRM_ACTIONS:
                self.domain_confirm_merge(entry_id)
                return entry, "合并已确认并冻结文件范围"

        entry, message = super().run_action(entry_id, action, payload)
        # 通用“启动合并”动作同时执行一次领域合并，落出 128 MiB 结果与冻结范围。
        if entry is not None and action == "启动合并":
            try:
                self.domain_merge_partitions(entry_id)
            except Exception:
                pass
        return entry, message

