"""存储优化：统一文件大小计算 + 合并交接 + 两种执行边界恢复。

完整对应题目样例：
合并确认 128 MiB（134217728 字节计划上限）→ 新增并提交 16 MiB →
压缩确认前中断 → 冻结边界按 128 MiB 重试 / 当前目录边界纳入 144 MiB；
重复恢复同一范围只生效一次，半写出文件不进入压缩，历史 128 MiB 可查。
"""
from __future__ import annotations

import sys

import pytest

sys.path.insert(0, '.')

from app.file_size import (  # noqa: E402
    mib_to_bytes,
    bytes_to_mib,
    file_size,
)
from app.services import service_for  # noqa: E402
from app.store import store  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_store():
    store.reset()
    yield
    store.reset()


class TestUnifiedFileSize:
    def test_mib_bytes_constants(self):
        assert mib_to_bytes(128) == 134217728
        assert mib_to_bytes(16) == 16777216
        assert mib_to_bytes(144) == 150994944
        assert bytes_to_mib(134217728) == 128

    def test_unified_result_carries_both_units(self):
        size = file_size(size_mib=128)
        assert size["size_mib"] == 128
        assert size["size_mb"] == 128
        assert size["size_bytes"] == 134217728

    def test_services_share_same_conversion(self):
        merge = service_for("partition_merge")
        comp = service_for("compaction_task")
        assert merge.mib_to_bytes(128) == comp.mib_to_bytes(128) == 134217728


class TestStorageOptimizeScenario:
    def test_full_narrative_frozen_then_current(self):
        merge = service_for("partition_merge")
        comp = service_for("compaction_task")

        # 原合并确认 128 MiB，旧字段/值保持
        result = merge.domain_merge_partitions(1)
        assert result["ok"] is True
        assert result["output_size_mb"] == 128
        assert result["output_size_bytes"] == 134217728

        # 合并后新增并提交 16 MiB
        new_file = merge.domain_add_file(1, size_mib=16, committed=True)
        assert new_file["size_bytes"] == 16777216

        # 半写出文件不能进入压缩
        half = merge.domain_add_file(1, size_mib=8, half_written=True)
        assert half["committed"] is False

        # 压缩规划保留 134217728 字节上限，交接核验一致
        plan = comp.domain_plan_compaction(1, merge_id=1)
        assert plan["target_file_size_bytes"] == 134217728
        assert plan["handoff_verification"]["consistent"] is True

        # 压缩确认前中断
        assert comp.domain_interrupt_compaction(1)["ok"] is True

        # 冻结边界：原重试继续使用 128 MiB
        frozen = comp.domain_resume_compaction(1, boundary="frozen")
        assert frozen["ok"] is True
        assert frozen["boundary"] == "frozen"
        assert frozen["total_size_mib"] == 128
        assert frozen["total_size_bytes"] == 134217728
        assert frozen["effective"] is True

        # 重复恢复同一冻结范围只生效一次
        frozen_again = comp.domain_resume_compaction(1, boundary="frozen")
        assert frozen_again["idempotent"] is True
        assert frozen_again["effective"] is False
        assert frozen_again["consumed_size_bytes"] == 134217728
        assert len(frozen_again["consumptions"]) == 1

        # 当前目录边界：纳入 144 MiB，但 128 不重复消费，只新增消费 16
        current = comp.domain_resume_compaction(1, boundary="current")
        assert current["boundary"] == "current"
        assert current["total_size_mib"] == 144
        assert current["total_size_bytes"] == 150994944
        assert current["newly_consumed_size_mib"] == 16
        assert current["consumed_size_bytes"] == 150994944
        assert current["duplicate_consumed_size_bytes"] == 0
        assert len(current["excluded_files"]) == 1  # 半写出文件
        assert len(current["new_files"]) == 1

        # 压缩待办：边界、已经使用、尚待纳入、两种边界完整记录
        todo = comp.domain_compaction_todo(1, merge_id=1)
        assert todo["adopted_boundary"] == "current"
        assert todo["boundaries"]["frozen"]["total_size_mib"] == 128
        assert todo["boundaries"]["current"]["total_size_mib"] == 144
        used_sizes = sorted(f["size_mib"] for f in todo["已经使用"])
        assert used_sizes == [16, 128]
        assert todo["尚待纳入"] == []
        assert todo["artifacts"]["frozen"]["size_bytes"] == 134217728
        assert todo["artifacts"]["current"]["size_bytes"] == 150994944

        entry = store.find("compaction_task", 1)
        assert entry["压缩待办"]["adopted_boundary"] == "current"

        # 历史 128 MiB 仍可查，旧调用仍返回 128
        history = merge.domain_merge_history(1)
        assert 128 in history["historical_size_mib"]
        assert 134217728 in history["historical_size_bytes"]
        again = merge.domain_merge_partitions(1)
        assert again["output_size_mb"] == 128

    def test_frozen_only_keeps_new_file_pending(self):
        merge = service_for("partition_merge")
        comp = service_for("compaction_task")
        merge.domain_merge_partitions(1)
        merge.domain_add_file(1, size_mib=16, committed=True)

        comp.domain_resume_compaction(1, boundary="frozen")
        todo = comp.domain_compaction_todo(1)
        assert todo["adopted_boundary"] == "frozen"
        assert todo["used_size_mib"] == 128
        assert todo["pending_size_mib"] == 16
        assert len(todo["尚待纳入"]) == 1
        assert todo["尚待纳入"][0]["size_mib"] == 16

    def test_repeated_same_scope_consumes_once(self):
        merge = service_for("partition_merge")
        comp = service_for("compaction_task")
        merge.domain_merge_partitions(1)

        first = comp.domain_resume_compaction(1, boundary="frozen")
        second = comp.domain_resume_compaction(1, boundary="frozen")
        third = comp.domain_recover_compaction(1, "frozen")
        assert first["effective"] is True
        assert second["idempotent"] is True
        assert third["idempotent"] is True
        assert len(third["consumptions"]) == 1
        assert third["consumed_size_bytes"] == 134217728

    def test_current_boundary_first_consumes_full_144(self):
        merge = service_for("partition_merge")
        comp = service_for("compaction_task")
        merge.domain_merge_partitions(1)
        merge.domain_add_file(1, size_mib=16, committed=True)

        current = comp.domain_resume_compaction(1, boundary="current")
        assert current["total_size_mib"] == 144
        assert current["newly_consumed_size_mib"] == 144
        # 之后冻结边界没有新对象，幂等且不重复消费
        frozen = comp.domain_resume_compaction(1, boundary="frozen")
        assert frozen["idempotent"] is True
        assert frozen["consumed_size_bytes"] == 150994944
