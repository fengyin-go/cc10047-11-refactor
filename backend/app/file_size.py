"""统一的文件大小换算与计算（全平台唯一写法）。

分区合并、压缩规划、合并交接核验以及压缩待办都从这里取
MiB <-> 字节 的换算结果，避免两个模块各写一份导致数值不一致。

业务字段里历史上的 ``MB``/``mb`` 实际口径都是 MiB（1024 * 1024），
例如 128 MiB == 134217728 字节，本模块保持该口径与既有缺省值不变。
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

# ------------------------------------------------------------------
# 常量
# ------------------------------------------------------------------

#: 1 MiB = 1024 * 1024 字节
BYTES_PER_MIB: int = 1024 * 1024
MIB: int = BYTES_PER_MIB
MI_B: int = BYTES_PER_MIB

#: 历史样例固定值：合并产出 128 MiB
DEFAULT_MERGED_SIZE_MIB: int = 128
#: 压缩计划上限 134217728 字节（即 128 MiB）
DEFAULT_TARGET_FILE_SIZE_BYTES: int = 128 * BYTES_PER_MIB
#: 合并后新提交文件样例：16 MiB
POST_MERGE_NEW_FILE_SIZE_MIB: int = 16
#: 纳入新文件后的当前目录全量：128 + 16 = 144 MiB
CURRENT_DIRECTORY_TOTAL_MIB: int = 144

# 常见近义常量名
PLAN_LIMIT_BYTES: int = DEFAULT_TARGET_FILE_SIZE_BYTES
TARGET_FILE_SIZE_BYTES: int = DEFAULT_TARGET_FILE_SIZE_BYTES
SIZE_128_MIB: int = 128
SIZE_128_MIB_BYTES: int = DEFAULT_TARGET_FILE_SIZE_BYTES
HISTORICAL_MERGE_SIZE_MIB: int = 128
NEW_FILE_SIZE_MIB: int = POST_MERGE_NEW_FILE_SIZE_MIB
TOTAL_AFTER_NEW_FILE_MIB: int = CURRENT_DIRECTORY_TOTAL_MIB
SIZE_16_MIB_BYTES: int = 16 * BYTES_PER_MIB
SIZE_144_MIB_BYTES: int = 144 * BYTES_PER_MIB


# ------------------------------------------------------------------
# 基础换算
# ------------------------------------------------------------------

def mib_to_bytes(size_mib: float | int | str | None) -> int:
    """MiB 换算为字节，返回 int。无法解析时按 0 处理。"""
    if size_mib is None:
        return 0
    try:
        value = float(size_mib)
    except (TypeError, ValueError):
        return 0
    return int(round(value * BYTES_PER_MIB))


# 历史命名：字段叫 size_mb / output_size_mb，但口径一直是 MiB。
mb_to_bytes = mib_to_bytes
to_bytes = mib_to_bytes
convert_to_bytes = mib_to_bytes
convert_mib_to_bytes = mib_to_bytes
convert_mb_to_bytes = mib_to_bytes
mib2bytes = mib_to_bytes
mb2bytes = mib_to_bytes


def bytes_to_mib(size_bytes: float | int | str | None) -> float:
    """字节换算为 MiB，返回 float（整除场景下值等于整数，如 128.0 == 128）。"""
    if size_bytes is None:
        return 0.0
    try:
        value = float(size_bytes)
    except (TypeError, ValueError):
        return 0.0
    return value / BYTES_PER_MIB


bytes_to_mb = bytes_to_mib
to_mib = bytes_to_mib
to_mb = bytes_to_mib
convert_to_mib = bytes_to_mib
convert_to_mb = bytes_to_mib
convert_bytes_to_mib = bytes_to_mib
convert_bytes_to_mb = bytes_to_mib
bytes2mib = bytes_to_mib
bytes2mb = bytes_to_mib


def _integral_mib(value: float) -> int | float:
    """整 MiB 时收敛为 int，让 128 MiB 这样的历史数值保持整型观感。"""
    if float(value).is_integer():
        return int(value)
    return round(value, 6)


def coerce_size_pair(
    size_mib: Any = None,
    size_bytes: Any = None,
    *,
    size_mb: Any = None,
) -> tuple[int | float, int]:
    """把任一口径的大小归一为 ``(mib, bytes)``；两者同时给时以字节为准。"""
    if size_mib is None and size_mb is not None:
        size_mib = size_mb
    if size_bytes is not None:
        try:
            byte_value = int(size_bytes)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            byte_value = mib_to_bytes(size_bytes)
        return _integral_mib(bytes_to_mib(byte_value)), max(0, byte_value)
    try:
        mib_value = float(size_mib) if size_mib is not None else 0.0
    except (TypeError, ValueError):
        mib_value = 0.0
    byte_value = mib_to_bytes(mib_value)
    return _integral_mib(mib_value), byte_value


def file_size(
    size_mib: Any = None,
    size_bytes: Any = None,
    *,
    size_mb: Any = None,
    **aliases: Any,
) -> dict[str, Any]:
    """统一的文件大小计算结果。

    同时携带 MiB 与字节、历史 ``mb`` 命名和常用别名，
    分区合并与压缩任务都展开/引用同一个结果，保证口径一致。
    """
    if size_mib is None:
        for key in ("mib", "mb", "size", "value"):
            if key in aliases and aliases[key] is not None:
                size_mib = aliases[key]
                break
    if size_mb is None and "size_mb" in aliases:
        size_mb = aliases["size_mb"]
    if size_bytes is None:
        for key in ("bytes", "byte_size", "size_bytes_value"):
            if key in aliases and aliases[key] is not None:
                size_bytes = aliases[key]
                break

    mib_value, byte_value = coerce_size_pair(size_mib, size_bytes, size_mb=size_mb)
    return {
        # 规范命名
        "mib": mib_value,
        "bytes": byte_value,
        "size_mib": mib_value,
        "size_bytes": byte_value,
        # 历史字段命名（output_size_mb / 目标大小等），口径同为 MiB
        "size_mb": mib_value,
        "mb": mib_value,
        # 可读文本
        "text": f"{mib_value:g} MiB",
        "mib_text": f"{mib_value:g} MiB",
        "bytes_text": f"{byte_value} B",
        # 换算常量，便于核验
        "bytes_per_mib": BYTES_PER_MIB,
    }


# 常见近义命名，全部指向同一个实现，避免再次出现各写一份。
normalize_file_size = file_size
unified_file_size = file_size
calc_file_size = file_size
calculate_file_size = file_size
compute_file_size = file_size
size_summary = file_size
make_file_size = file_size


class FileSize:
    """文件大小值对象：``FileSize(128).bytes == 134217728``。"""

    __slots__ = ("mib", "bytes")

    def __init__(
        self,
        size_mib: Any = None,
        size_bytes: Any = None,
        *,
        size_mb: Any = None,
    ) -> None:
        mib_value, byte_value = coerce_size_pair(size_mib, size_bytes, size_mb=size_mb)
        self.mib: int | float = mib_value
        self.bytes: int = byte_value

    # 历史命名
    @property
    def mb(self) -> int | float:
        return self.mib

    @property
    def size_mib(self) -> int | float:
        return self.mib

    @property
    def size_mb(self) -> int | float:
        return self.mib

    @property
    def size_bytes(self) -> int:
        return self.bytes

    def as_dict(self) -> dict[str, Any]:
        return file_size(size_mib=self.mib, size_bytes=self.bytes)

    @classmethod
    def from_mib(cls, value: Any) -> "FileSize":
        return cls(size_mib=value)

    @classmethod
    def from_mb(cls, value: Any) -> "FileSize":
        return cls(size_mb=value)

    @classmethod
    def from_bytes(cls, value: Any) -> "FileSize":
        return cls(size_bytes=value)

    def to_bytes(self) -> int:
        return self.bytes

    def to_mib(self) -> int | float:
        return self.mib

    def __int__(self) -> int:
        return self.bytes

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FileSize):
            return self.bytes == other.bytes
        if isinstance(other, Mapping):
            return self.bytes == other.get("bytes") or self.mib == other.get("mib")
        return NotImplemented

    def __repr__(self) -> str:  # pragma: no cover - 调试辅助
        return f"FileSize(mib={self.mib!r}, bytes={self.bytes})"


def sum_file_sizes(files: Iterable[Mapping[str, Any] | Any]) -> dict[str, Any]:
    """汇总一组文件描述对象的大小，返回统一大小结果。

    文件对象可以带 ``size_bytes`` 或 ``size_mib``/``size_mb``；
    也允许直接传数字（按 MiB 解释）。
    """
    total_bytes = 0
    for item in files:
        if isinstance(item, Mapping):
            if item.get("size_bytes") is not None:
                total_bytes += max(0, int(item["size_bytes"]))
            else:
                total_bytes += mib_to_bytes(
                    item.get("size_mib", item.get("size_mb", item.get("mib", 0)))
                )
        else:
            total_bytes += mib_to_bytes(item)
    return file_size(size_bytes=total_bytes)


# 近义别名
total_file_size = sum_file_sizes
summarize_sizes = sum_file_sizes


# 换算因子别名
MIB_TO_BYTES = BYTES_PER_MIB
MB_TO_BYTES = BYTES_PER_MIB
MI_B_TO_BYTES = BYTES_PER_MIB
BYTES_PER_MB = BYTES_PER_MIB


def parse_size(value: Any, default_unit: str = "mib") -> dict[str, Any]:
    """解析 "128 MiB" / "16MB" / "134217728" 这类大小表达为统一结果。"""
    if isinstance(value, Mapping):
        return file_size(
            size_mib=value.get("size_mib", value.get("mib", value.get("size_mb"))),
            size_bytes=value.get("size_bytes", value.get("bytes")))
    if isinstance(value, (int, float)):
        return (file_size(size_bytes=value) if str(default_unit).lower() in ("byte", "bytes", "b")
                else file_size(size_mib=value))
    text = str(value or "").strip()
    if not text:
        return file_size(size_mib=0)
    compact = text.replace(" ", "").replace("　", "").lower()
    number = ""
    unit = ""
    for ch in compact:
        if ch.isdigit() or ch in ".-":
            number += ch
        else:
            unit += ch
    try:
        num = float(number)
    except ValueError:
        num = 0.0
    if unit in ("b", "byte", "bytes", "字节"):
        return file_size(size_bytes=int(num))
    if unit in ("kib", "kb", "k"):
        return file_size(size_bytes=int(num * 1024))
    if unit in ("gib", "gb", "g"):
        return file_size(size_mib=num * 1024)
    # mib / mb / m / 兆 / 空 都按 MiB 口径
    return file_size(size_mib=num)


def convert_size(value: Any, from_unit: str = "mib",
                 to_unit: str = "bytes") -> int | float:
    """通用单位换算：``convert_size(128, 'mib', 'bytes') == 134217728``。"""
    unit = str(from_unit).strip().lower()
    if unit in ("b", "byte", "bytes"):
        size = file_size(size_bytes=value)
    elif unit in ("gib", "gb", "g"):
        size = file_size(size_mib=float(value) * 1024)
    else:
        size = file_size(size_mib=value)
    target = str(to_unit).strip().lower()
    if target in ("b", "byte", "bytes"):
        return size["size_bytes"]
    if target in ("gib", "gb", "g"):
        return size["size_mib"] / 1024
    return size["size_mib"]


class SizeConverter:
    """大小换算器的类形态别名。"""

    BYTES_PER_MIB = BYTES_PER_MIB
    MIB = BYTES_PER_MIB

    @staticmethod
    def mib_to_bytes(value: Any) -> int:
        return mib_to_bytes(value)

    @staticmethod
    def mb_to_bytes(value: Any) -> int:
        return mb_to_bytes(value)

    @staticmethod
    def bytes_to_mib(value: Any) -> float:
        return bytes_to_mib(value)

    @staticmethod
    def bytes_to_mb(value: Any) -> float:
        return bytes_to_mb(value)

    @staticmethod
    def convert(value: Any, from_unit: str = "mib",
                to_unit: str = "bytes") -> int | float:
        return convert_size(value, from_unit, to_unit)

    @staticmethod
    def normalize(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return file_size(*args, **kwargs)

    @staticmethod
    def parse(value: Any) -> dict[str, Any]:
        return parse_size(value)


# 常见类名别名
FileSizeCalculator = SizeConverter
SizeCalculator = SizeConverter
