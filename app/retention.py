"""模块 7a · 各来源保留边界（纯函数 + 来源水位线登记）。

背景：已撤销订单的墓碑长期留在快照/导出里导致存储膨胀，但清理后又可能被补传的
低版本记录写回。能不能清，必须由**各来源共同确认**：

- 每个上游来源（如 binlog 采集、消息队列、批量补传通道）各自上报自己的安全水位线
  （source watermark）：该来源保证不会再补传 ts <= 水位线的事件。
- 共同保留边界（joint retention boundary）= min(所有已登记来源的水位线)。
  取最小是保守口径：只要还有一个来源可能补传旧事件，边界就不能越过它。
- 墓碑只有在「墓碑 ts <= 共同保留边界」时才允许压缩；边界之上的墓碑一律保留。

登记口径（强约束，出错显式拒绝，不静默降级）：
- 至少要有 1 个来源；来源名非空。
- 水位线必须是可解析的 ISO 时间字符串；解析失败抛 RetentionError。
- 同一来源重复登记取更晚者（水位线只许前进，回退会被拒绝）。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .archive import EventError, parse_ts


class RetentionError(ValueError):
    """来源水位线不合法（无来源 / 时间不可解析 / 水位线回退）。"""


@dataclass(frozen=True)
class SourceWatermark:
    """单个来源登记的安全水位线。"""

    source: str
    watermark: datetime
    note: str = ""

    @property
    def watermark_iso(self) -> str:
        return self.watermark.strftime("%Y-%m-%dT%H:%M:%SZ")

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "watermark": self.watermark_iso,
            "note": self.note,
        }


def register_source(
    sources: dict[str, SourceWatermark],
    source: str,
    raw_watermark: str,
    note: str = "",
) -> SourceWatermark:
    """把一个来源的水位线登记进集合；返回登记后的水位线对象。

    - 来源名为空 / 水位线不可解析 → RetentionError。
    - 同一来源重复登记：新水位线必须 >= 旧水位线，回退显式拒绝。
    """
    if not isinstance(source, str) or not source.strip():
        raise RetentionError("来源名必须是非空字符串")
    name = source.strip()
    try:
        mark = parse_ts(raw_watermark)
    except EventError as exc:
        raise RetentionError(f"来源 {name} 的水位线无法解析：{raw_watermark!r}（{exc}）") from exc
    current = sources.get(name)
    if current is not None and mark < current.watermark:
        raise RetentionError(
            f"来源 {name} 的水位线不允许回退：已登记 {current.watermark_iso}，"
            f"本次收到 {mark.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
    entry = SourceWatermark(source=name, watermark=mark, note=note)
    sources[name] = entry
    return entry


def joint_boundary(sources: dict[str, SourceWatermark]) -> datetime | None:
    """共同保留边界 = min(各来源水位线)；一个来源都没有时返回 None。"""
    if not sources:
        return None
    return min(item.watermark for item in sources.values())


def boundary_views(sources: dict[str, SourceWatermark]) -> dict:
    """结果页/快照用：逐来源水位线 + 共同边界 + 覆盖来源清单。"""
    if not sources:
        return {
            "sources": [],
            "boundary": None,
            "source_count": 0,
            "rule": "min(各来源安全水位线)；无来源登记时不允许压缩任何墓碑",
        }
    boundary = joint_boundary(sources)
    ordered = sorted(sources.values(), key=lambda item: (item.watermark, item.source))
    return {
        "sources": [item.to_dict() for item in ordered],
        "boundary": boundary.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "boundary_source": ordered[0].source,  # 持有最小值（最慢）的来源
        "source_count": len(ordered),
        "rule": "min(各来源安全水位线)；墓碑 ts <= 共同边界才允许压缩",
    }


def tombstone_votes(
    tombstone_ts: datetime,
    sources: dict[str, SourceWatermark],
) -> dict:
    """每个来源对单个墓碑的保留/放行投票，供结果页逐条展示依据。

    来源水位线 >= 墓碑 ts → 该来源确认不会再补传更旧事件 → vote=release（同意压缩）；
    否则 vote=retain（仍可能补传，要求保留）。
    """
    boundary = joint_boundary(sources)
    votes = []
    for item in sorted(sources.values(), key=lambda s: (s.watermark, s.source)):
        votes.append(
            {
                "source": item.source,
                "watermark": item.watermark_iso,
                "vote": "release" if item.watermark >= tombstone_ts else "retain",
            }
        )
    eligible = boundary is not None and tombstone_ts <= boundary
    return {
        "votes": votes,
        "eligible": eligible,
        "deciding_source": votes[0]["source"] if votes else None,
    }
