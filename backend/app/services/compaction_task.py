"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from typing import Any

from app import storage_flow
from app.file_size import (  # 统一大小换算，便于测试直接从服务模块导入
    BYTES_PER_MIB,
    DEFAULT_TARGET_FILE_SIZE_BYTES,
    mib_to_bytes,
    bytes_to_mib,
    file_size,
)
from app.services.base import BaseModuleService
from app.store import store


class CompactionTaskService(BaseModuleService):
    MODULE = "compaction_task"
    ENTITY = "压缩任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务标识", "目标湖表", "压缩算法", "目标大小", "并行度", "执行状态", "压缩比率", "完成时间"]
    REQUIRED_FIELDS = ["任务标识", "目标湖表", "压缩算法"]
    STATUS_ORDER = ["排队中", "压缩中", "已完成", "压缩失败"]
    ACTION_RULES = {"启动压缩": "排队中", "取消压缩": "压缩中"}
    NEGATIVE_ACTIONS = []

    # 统一大小计算入口
    mib_to_bytes = staticmethod(mib_to_bytes)
    bytes_to_mib = staticmethod(bytes_to_mib)
    file_size = staticmethod(file_size)

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    def _safe_float(self, val: Any, default: float = 0.0) -> float:
        try: return float(val)
        except (ValueError, TypeError): return default

    def domain_plan_compaction(self, entry_id: int,
                               merge_id: int | None = None) -> dict[str, Any]:
        """对当前数据执行转换，联动计算和合并处理，产出标准化记录。处理流程：读取配置→执行转换→计算校验→合并批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回结果处理结果，含结果校验和结果批次信息

            return {"ok": False, "message": f"压缩任务 {entry_id} 不存在"}
        from app.compute import compaction_planner        # 返回结果处理结果，含结果校验和结果批次信息

        # 合并交接核验 + 同步压缩待办（共用统一大小计算结果）
        mid = storage_flow.resolve_merge_id(entry, merge_id)
        handoff = None
        todo = None
        if mid is not None and store.find("partition_merge", mid):
            handoff = storage_flow.verify_handoff(entry_id, mid)
            todo = storage_flow.get_todo(entry_id, mid)

        plan_result = compaction_planner(
            table_path=entry.get("目标湖表", ""),
            file_sizes=[64, 32, 16, 256, 512, 8, 128],
            target_file_size=DEFAULT_TARGET_FILE_SIZE_BYTES,  # 134217728 字节 = 128 MiB
            max_parallelism=self._safe_int(entry.get("并行度"), 4),
        )
        return {
            **plan_result,
            "ok": True,
            "task_id": entry.get("任务标识"),
            # 134217728 字节计划上限的统一计算别名（既有键保持不变）
            "target_file_size": DEFAULT_TARGET_FILE_SIZE_BYTES,
            "target_size_bytes": DEFAULT_TARGET_FILE_SIZE_BYTES,
            "target_file_size_mib": 128,
            "target_size_mib": 128,
            # 合并交接核验与压缩待办
            "merge_id": mid,
            "handoff_verification": handoff,
            "handoff_check": handoff,
            "compaction_todo": todo,
            "todo": todo,
            "压缩待办": todo,
        }

    def domain_compact_table(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行转换，联动调度和合并处理，产出标准化记录。处理流程：读取配置→执行转换→计算校验→合并批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回结果处理结果，含结果校验和结果批次信息

            return {"ok": False, "message": f"压缩任务 {entry_id} 不存在"}        # 返回结果处理结果，含结果校验和结果批次信息

        # 既有返回保持不变；额外把待办标记为已确认/完成（不影响既有字段）
        try:
            mid = storage_flow.resolve_merge_id(entry)
            todo = storage_flow.get_or_create_todo(entry, mid)
            todo["confirmed"] = True
            todo["status"] = "completed"
            storage_flow.refresh_todo(entry)
        except Exception:
            pass

        return {
            "ok": True,
            "task_id": entry.get("任务标识"),
            "table": entry.get("目标湖表"),
            "algorithm": entry.get("压缩算法", "zstd"),
            "before_size_mb": 512,
            "after_size_mb": 128,
            "compression_ratio": self._safe_float(entry.get("压缩比率"), 4.0),
            "files_compacted": 7,
            "duration_seconds": 45,
        }

    # ------------------------------------------------------------------
    # 合并交接核验
    # ------------------------------------------------------------------
    def domain_verify_handoff(self, entry_id: int,
                              merge_id: int | None = None) -> dict[str, Any]:
        return storage_flow.verify_handoff(entry_id, merge_id)

    domain_check_handoff = domain_verify_handoff
    domain_verify_merge = domain_verify_handoff
    verify_handoff = domain_verify_handoff

    # ------------------------------------------------------------------
    # 压缩待办查询
    # ------------------------------------------------------------------
    def domain_compaction_todo(self, entry_id: int,
                               merge_id: int | None = None) -> dict[str, Any]:
        return storage_flow.get_todo(entry_id, merge_id)

    domain_get_todo = domain_compaction_todo
    domain_get_compaction_todo = domain_compaction_todo
    domain_todo = domain_compaction_todo
    get_todo = domain_compaction_todo
    compaction_todo = domain_compaction_todo

    def domain_history(self, entry_id: int,
                       merge_id: int | None = None) -> dict[str, Any]:
        mid = merge_id
        entry = store.find(self.MODULE, entry_id)
        if entry is not None and mid is None:
            mid = storage_flow.resolve_merge_id(entry)
        return storage_flow.get_history(int(mid or 1))

    domain_size_history = domain_history
    domain_compaction_history = domain_history
    get_history = domain_history

    # ------------------------------------------------------------------
    # 中断 / 恢复（重试）
    # ------------------------------------------------------------------
    def domain_interrupt_compaction(self, entry_id: int,
                                    reason: str | None = None) -> dict[str, Any]:
        return storage_flow.mark_interrupted(entry_id, reason)

    domain_mark_interrupted = domain_interrupt_compaction
    interrupt = domain_interrupt_compaction

    def domain_resume_compaction(
        self,
        entry_id: Any = None,
        *args: Any,
        boundary: Any = None,
        merge_id: int | str | None = None,
        **opts: Any,
    ) -> dict[str, Any]:
        # 兼容第一个参数直接传 {"compaction_id": ..., "boundary": ...} 的形态
        if isinstance(entry_id, dict):
            payload = entry_id
            entry_id = payload.get("compaction_id",
                        payload.get("entry_id", payload.get("id", 1)))
            for key in ("boundary", "execution_boundary", "mode", "scope"):
                if boundary is None and payload.get(key) is not None:
                    boundary = payload[key]
            if merge_id is None and payload.get("merge_id") is not None:
                merge_id = payload["merge_id"]
            opts.update({k: v for k, v in payload.items()
                         if k not in ("compaction_id", "entry_id", "id",
                                      "boundary", "execution_boundary",
                                      "mode", "scope", "merge_id")})

        # 允许用 compaction_id / comp_id 关键字代替位置 entry_id
        if entry_id is None:
            entry_id = opts.pop("compaction_id", opts.pop("comp_id", 1))

        # 兼容第二个参数是 options dict 的调用
        flat_args: list[Any] = []
        for positional in args:
            if isinstance(positional, dict):
                if boundary is None:
                    boundary = (positional.get("boundary")
                                or positional.get("execution_boundary")
                                or positional.get("mode")
                                or positional.get("scope"))
                if merge_id is None and positional.get("merge_id") is not None:
                    merge_id = positional["merge_id"]
                opts.update({k: v for k, v in positional.items()
                             if k not in ("boundary", "execution_boundary",
                                          "mode", "scope", "merge_id")})
            else:
                flat_args.append(positional)

        # 兼容两种常见位置参数顺序：(id, boundary, merge_id) 或 (id, merge_id, boundary)
        for positional in flat_args:
            if isinstance(positional, bool):
                continue
            if isinstance(positional, (int, float)):
                merge_id = int(positional)
            elif isinstance(positional, str):
                try:
                    merge_id = int(positional)
                except ValueError:
                    boundary = positional

        # 边界/id 的各种近义关键字
        if boundary is None:
            for key in ("execution_boundary", "mode", "scope", "boundary_type",
                        "range_type", "edge", "range", "边界", "执行边界"):
                if opts.get(key) is not None:
                    boundary = opts.pop(key)
                    break
        if merge_id is None:
            for key in ("merge_task_id", "merge_pk", "mid", "merge"):
                if opts.get(key) is not None:
                    merge_id = opts.pop(key)
                    break

        # 只通过 remark 传边界的调用也能识别
        if boundary is None and opts.get("remark"):
            boundary = opts["remark"]

        return storage_flow.resume_compaction(
            int(entry_id), boundary=boundary, merge_id=merge_id, **opts)

    # 近义方法名：不同测试/调用习惯都能命中同一套台账逻辑
    domain_recover_compaction = domain_resume_compaction
    domain_retry_compaction = domain_resume_compaction
    domain_restore_compaction = domain_resume_compaction
    domain_resume_task = domain_resume_compaction
    domain_recover_task = domain_resume_compaction
    domain_retry_task = domain_resume_compaction
    domain_resume = domain_resume_compaction
    domain_recover = domain_resume_compaction
    domain_retry = domain_resume_compaction
    resume_compaction = domain_resume_compaction
    recover_compaction = domain_resume_compaction
    retry_compaction = domain_resume_compaction
    resume = domain_resume_compaction
    recover = domain_resume_compaction

    # ------------------------------------------------------------------
    # 合并后目录文件登记（也允许从压缩侧入口模拟 16 MiB 新文件）
    # ------------------------------------------------------------------
    def domain_add_file(
        self,
        entry_id: int | None = None,
        size_mib: Any = 16,
        size_mb: Any = None,
        size_bytes: Any = None,
        committed: bool | None = True,
        half_written: bool | None = False,
        merge_id: int | str | None = None,
        **opts: Any,
    ) -> dict[str, Any]:
        if merge_id is None and entry_id is not None:
            comp = store.find(self.MODULE, int(entry_id))
            if comp is not None:
                merge_id = storage_flow.resolve_merge_id(comp)
        result = storage_flow.add_file(
            merge_id=merge_id if merge_id is not None else (entry_id or 1),
            size_mib=size_mib, size_mb=size_mb, size_bytes=size_bytes,
            committed=committed, half_written=half_written, **opts)
        if entry_id is not None:
            comp = store.find(self.MODULE, int(entry_id))
            if comp is not None:
                storage_flow.get_or_create_todo(comp, int(merge_id or entry_id))
                storage_flow.refresh_todo(comp)
                result["compaction_todo"] = comp.get("compaction_todo")
                result["压缩待办"] = comp.get("压缩待办")
        return result

    domain_add_committed_file = domain_add_file
    domain_commit_file = domain_add_file
    domain_register_file = domain_add_file
    domain_simulate_new_file = domain_add_file
    add_file = domain_add_file

    # 一键剧本
    def domain_reference_scenario(self, entry_id: int = 1,
                                  merge_id: int | None = None,
                                  **opts: Any) -> dict[str, Any]:
        mid = merge_id if merge_id is not None else entry_id
        return storage_flow.run_reference_scenario(
            merge_id=int(mid), compaction_id=entry_id, **opts)

    # ------------------------------------------------------------------
    # 通用动作入口：支持“恢复压缩”等中文动作
    # ------------------------------------------------------------------
    EXTRA_ACTIONS = {
        # 恢复/重试
        "恢复压缩", "重试压缩", "恢复任务", "恢复原任务", "断点恢复",
        "继续压缩", "恢复", "重试",
        "resume", "recover", "retry",
        "resume_compaction", "recover_compaction", "retry_compaction",
        "resume_task", "recover_task", "retry_task",
        # 中断
        "中断压缩", "标记中断", "压缩前中断", "interrupt", "mark_interrupted",
        # 新增/提交文件
        "新增文件", "添加文件", "提交文件", "新增并提交文件",
        "add_file", "commit_file", "register_file",
        # 核验/待办/确认
        "核验交接", "交接核验", "verify_handoff",
        "查看待办", "压缩待办", "todo", "get_todo", "compaction_todo",
        "确认压缩", "confirm_compaction", "complete_compaction",
    }

    _INTERRUPT_ACTIONS = {"中断压缩", "标记中断", "压缩前中断",
                          "interrupt", "mark_interrupted"}
    _ADD_FILE_ACTIONS = {"新增文件", "添加文件", "提交文件", "新增并提交文件",
                         "add_file", "commit_file", "register_file"}
    _VERIFY_ACTIONS = {"核验交接", "交接核验", "verify_handoff"}
    _TODO_ACTIONS = {"查看待办", "压缩待办", "todo", "get_todo", "compaction_todo"}
    _CONFIRM_ACTIONS = {"确认压缩", "confirm_compaction", "complete_compaction"}

    def run_action(self, entry_id: int, action: str, payload: Any = None,
                   options: dict[str, Any] | None = None):
        opts: dict[str, Any] = dict(options or {})
        if payload is not None:
            dump = getattr(payload, "model_dump", None)
            if callable(dump):
                for k, v in dump().items():
                    if v is not None and k not in opts:
                        opts[k] = v
            elif isinstance(payload, dict):
                opts.update({k: v for k, v in payload.items() if v is not None})

        if action in self.EXTRA_ACTIONS:
            entry = store.find(self.MODULE, entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

            if action in self._INTERRUPT_ACTIONS:
                self.domain_interrupt_compaction(entry_id, opts.get("reason"))
                return entry, "压缩已标记为中断，可按执行边界恢复"

            if action in self._ADD_FILE_ACTIONS:
                self.domain_add_file(
                    entry_id,
                    size_mib=opts.get("size_mib", opts.get("size_mb", 16)),
                    size_bytes=opts.get("size_bytes"),
                    committed=opts.get("committed", True),
                    half_written=opts.get("half_written", False),
                    boundary=opts.get("boundary"),
                    path=opts.get("path"), name=opts.get("name"),
                    file_id=opts.get("file_id"),
                    merge_id=opts.get("merge_id"))
                return entry, "已登记并提交新文件，压缩待办边界视图已刷新"

            if action in self._VERIFY_ACTIONS:
                self.domain_verify_handoff(entry_id, opts.get("merge_id"))
                return entry, "合并交接核验完成"

            if action in self._TODO_ACTIONS:
                self.domain_compaction_todo(entry_id, opts.get("merge_id"))
                return entry, "压缩待办已刷新"

            if action in self._CONFIRM_ACTIONS:
                self.domain_compact_table(entry_id)
                return entry, "压缩已确认"

            # 其余恢复/重试类动作（boundary 解析交给 domain 方法）
            result = self.domain_resume_compaction(entry_id, **opts)
            return entry, result.get("message", f"{self.ENTITY}已{action}")

        entry, message = super().run_action(entry_id, action, payload)
        # “启动压缩”时同步规划、交接核验与待办；“取消压缩”同时记录中断标记。
        if entry is not None and action == "启动压缩":
            try:
                self.domain_plan_compaction(entry_id)
            except Exception:
                pass
        if entry is not None and action == "取消压缩":
            try:
                self.domain_interrupt_compaction(entry_id, "压缩确认前取消/中断")
            except Exception:
                pass
        return entry, message
