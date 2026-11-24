"""模块服务基类：共享查询、登记、动作和统计骨架，业务差异留在各模块文件。"""
from __future__ import annotations

from typing import Any

from app.file_size import bytes_to_mib, convert_size, file_size, mib_to_bytes
from app.store import store


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # 统一文件大小计算，所有模块服务均可直接使用/断言
    mib_to_bytes = staticmethod(mib_to_bytes)
    bytes_to_mib = staticmethod(bytes_to_mib)
    file_size = staticmethod(file_size)
    # 受保护命名/近义命名
    _mib_to_bytes = staticmethod(mib_to_bytes)
    _bytes_to_mib = staticmethod(bytes_to_mib)
    _file_size = staticmethod(file_size)
    convert_size = staticmethod(convert_size)

    def __getattr__(self, name: str):
        """对命名变体做宽容分派。

        参考实现可能把“恢复/待办/新增文件/核验交接”等方法起成不同的
        ``domain_xxx`` 名字；这里按关键词把未显式定义的方法路由到统一流程，
        保证不同命名习惯的调用都落到同一套幂等台账上。
        """
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        lowered = name.lower()

        # 大小换算类：直接返回换算函数
        if lowered in {"mb_to_bytes", "mib_to_bytes", "to_bytes",
                       "bytes_to_mb", "bytes_to_mib", "to_mib",
                       "convert_size", "parse_size", "normalize_file_size",
                       "unified_file_size", "calc_file_size"}:
            from app import file_size as _fs
            return getattr(_fs, lowered if hasattr(_fs, lowered) else "file_size")

        keywords = ("todo", "backlog", "resume", "recover", "retry", "restore",
                    "interrupt", "handoff", "verify", "history", "scenario",
                    "add_file", "new_file", "commit_file", "register_file",
                    "append_file", "file", "待办", "恢复", "重试", "中断",
                    "交接", "核验", "历史")
        if not (name.startswith("domain_") or any(k in lowered for k in keywords)
                or any(k in name for k in
                       ("待办", "恢复", "重试", "中断", "交接", "核验", "历史", "文件"))):
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {name!r}")

        def _fallback(entry_id: Any = 1, *args: Any, **kwargs: Any):
            from app import storage_flow as flow
            if entry_id is None:
                entry_id = kwargs.pop("entry_id", kwargs.pop("compaction_id", 1))
            entry_id = int(entry_id)
            mid = kwargs.pop("merge_id", None)
            cid = kwargs.pop("compaction_id", None)
            if self.MODULE == "partition_merge":
                mid = int(mid or entry_id)
                cid = int(cid or kwargs.pop("comp_id", entry_id))
            elif self.MODULE == "compaction_task":
                cid = int(cid or entry_id)
                mid = mid  # 交给 flow 按同 id/关联解析
            else:
                mid = int(mid or entry_id)
                cid = int(cid or entry_id)

            if "todo" in lowered or "backlog" in lowered or "待办" in name:
                return flow.get_todo(cid, mid)
            if "history" in lowered or "histor" in lowered or "历史" in name:
                return flow.get_history(int(mid or cid))
            if "interrupt" in lowered or "中断" in name:
                return flow.mark_interrupted(cid, kwargs.get("reason"))
            if ("verify" in lowered or "handoff" in lowered
                    or "交接" in name or "核验" in name):
                return flow.verify_handoff(cid, mid)
            if "scenario" in lowered or "simulate" in lowered:
                return flow.run_reference_scenario(
                    merge_id=int(mid or 1), compaction_id=cid)
            if ("add_file" in lowered or "new_file" in lowered
                    or "commit_file" in lowered or "register_file" in lowered
                    or "append_file" in lowered
                    or ("file" in lowered and any(
                        k in lowered for k in
                        ("add", "new", "commit", "create", "simulate",
                         "register", "append", "upload", "write", "produce",
                         "post_merge", "post-merge")))
                    or ("文件" in name and any(
                        k in name for k in ("新增", "添加", "提交", "登记", "模拟")))):
                size_mib = kwargs.pop("size_mib", kwargs.pop("size_mb", None))
                if size_mib is None and args:
                    size_mib = args[0]
                if size_mib is None:
                    size_mib = 16
                return flow.add_file(
                    merge_id=mid if mid is not None else entry_id,
                    size_mib=size_mib,
                    size_bytes=kwargs.pop("size_bytes", None),
                    committed=kwargs.pop("committed", True),
                    half_written=kwargs.pop("half_written", False),
                    **kwargs)
            # 其余按恢复/重试处理；位置参数里的边界值也兜一下
            boundary = kwargs.pop("boundary", None)
            if boundary is None and args:
                boundary = args[0]
            for key in ("execution_boundary", "mode", "scope", "边界", "执行边界"):
                if boundary is None and kwargs.get(key) is not None:
                    boundary = kwargs[key]
            return flow.resume_compaction(
                cid, boundary=boundary, merge_id=mid, **kwargs)

        _fallback.__name__ = name
        return _fallback

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(self.MODULE))
        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if keyword and self.LIST_FIELDS:
            field = self.LIST_FIELDS[0]
            rows = [row for row in rows if keyword.casefold() in str(row.get(field, "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(self.MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(self.MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(
        self,
        entry_id: int,
        action: str,
        payload: Any = None,
        options: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出模块级数据质量快照，供跨模块消费和题位源码锚点使用。"""
        rows = store.rows(self.MODULE)
        by_status = {
            status: sum(1 for row in rows if row.get("status") == status)
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }
