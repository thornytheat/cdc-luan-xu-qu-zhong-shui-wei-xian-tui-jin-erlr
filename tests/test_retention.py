"""模块 7：各来源保留边界（retention）纯函数测试。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.archive import parse_ts
from app.retention import (
    RetentionError,
    SourceWatermark,
    boundary_views,
    joint_boundary,
    register_source,
    tombstone_votes,
)


def ts(text: str) -> datetime:
    return parse_ts(text)


def test_joint_boundary_is_min_of_all_sources():
    sources: dict[str, SourceWatermark] = {}
    register_source(sources, "binlog", "2026-11-10T00:45:00Z")
    register_source(sources, "mq", "2026-11-10T00:40:00Z")
    register_source(sources, "backfill", "2026-11-10T00:50:00Z")
    assert joint_boundary(sources) == ts("2026-11-10T00:40:00Z")
    view = boundary_views(sources)
    assert view["boundary"] == "2026-11-10T00:40:00Z"
    assert view["boundary_source"] == "mq"  # 最慢来源持有共同边界
    assert view["source_count"] == 3


def test_no_source_means_no_boundary_and_no_compaction():
    assert joint_boundary({}) is None
    view = boundary_views({})
    assert view["boundary"] is None
    assert view["sources"] == []


def test_watermark_rollback_is_rejected():
    sources: dict[str, SourceWatermark] = {}
    register_source(sources, "binlog", "2026-11-10T00:45:00Z")
    with pytest.raises(RetentionError, match="不允许回退"):
        register_source(sources, "binlog", "2026-11-10T00:44:00Z")
    # 相等时间允许（幂等推进）
    register_source(sources, "binlog", "2026-11-10T00:45:00Z")
    assert joint_boundary(sources) == ts("2026-11-10T00:45:00Z")


def test_invalid_watermark_and_blank_source_rejected():
    sources: dict[str, SourceWatermark] = {}
    with pytest.raises(RetentionError, match="无法解析"):
        register_source(sources, "binlog", "not-a-timestamp")
    with pytest.raises(RetentionError, match="来源名"):
        register_source(sources, "  ", "2026-11-10T00:45:00Z")


def test_tombstone_votes_require_every_source_to_release():
    sources: dict[str, SourceWatermark] = {}
    register_source(sources, "binlog", "2026-11-10T00:42:00Z")
    register_source(sources, "mq", "2026-11-10T00:40:00Z")
    # 墓碑 ts=00:42：binlog 放行，mq 要求保留 → 整体不合格
    poll = tombstone_votes(ts("2026-11-10T00:42:00Z"), sources)
    assert poll["eligible"] is False
    votes = {v["source"]: v["vote"] for v in poll["votes"]}
    assert votes == {"binlog": "release", "mq": "retain"}

    # 墓碑 ts=00:40：两来源水位线都 >= 墓碑 ts → 全部放行
    poll2 = tombstone_votes(ts("2026-11-10T00:40:00Z"), sources)
    assert poll2["eligible"] is True
    assert all(v["vote"] == "release" for v in poll2["votes"])
