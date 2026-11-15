"""模块 7：墓碑压缩计划、重放拦截与整批校验（compaction 纯函数）。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app import compaction
from app.archive import Event
from app.retention import SourceWatermark, register_source


def make_sources(*pairs) -> dict[str, SourceWatermark]:
    sources: dict[str, SourceWatermark] = {}
    for name, mark in pairs:
        register_source(sources, name, mark)
    return sources


def tomb_row(pk: str, version: int = 3, line: int = 57, ts: str = "2026-11-10T00:42:00Z") -> dict:
    return {
        "pk": pk,
        "tombstone_version": version,
        "tombstone_line_no": line,
        "ts": ts,
        "deleted": True,
    }


def test_plan_keeps_all_tombstones_when_no_source_registered():
    plan = compaction.build_table_plan("orders", [tomb_row("O1"), tomb_row("O2")], {})
    assert plan.tombstone_before == 2
    assert plan.eligible == []
    assert len(plan.retained) == 2
    assert all("保守口径" in d.reason for d in plan.decisions)


def test_plan_releases_only_tombstones_within_joint_boundary():
    sources = make_sources(("binlog", "2026-11-10T00:45:00Z"),
                           ("mq", "2026-11-10T00:42:00Z"))
    rows = [
        tomb_row("O_OLD", ts="2026-11-10T00:42:00Z"),  # 等于边界，可压缩
        tomb_row("O_NEW", ts="2026-11-10T00:43:00Z"),  # 高于边界，保留
    ]
    plan = compaction.build_table_plan("orders", rows, sources)
    assert [d.pk for d in plan.eligible] == ["O_OLD"]
    assert [d.pk for d in plan.retained] == ["O_NEW"]
    assert plan.to_dict()["tombstone_after"] == 1
    # 放行项带逐来源投票
    votes = {v["source"]: v["vote"] for v in plan.eligible[0].votes}
    assert votes == {"binlog": "release", "mq": "release"}


def test_replay_classification_blocks_low_or_equal_version_and_allows_higher():
    compacted = {"tombstone_version": 3, "tombstone_line_no": 57}

    def event(version: int) -> Event:
        return Event(
            table="orders", line_no=70, pk="O1", version=version,
            ts=datetime(2026, 11, 10, 1, 0, tzinfo=timezone.utc),
            op="u", after={"x": 1}, schema_version=1, source="orders.jsonl",
        )

    assert compaction.classify_replay(event(1), compacted) == "blocked_replay"
    assert compaction.classify_replay(event(3), compacted) == "blocked_replay"  # 等版本也拦截
    assert compaction.classify_replay(event(4), compacted) == "resurrect"
    assert compaction.classify_replay(event(1), None) is None  # 无压缩墓碑，不拦截
    assert "拦截且不复活" in compaction.replay_reason(event(2), compacted)


def test_verify_batch_passes_when_all_tables_consistent():
    sources = make_sources(("binlog", "2026-11-10T00:45:00Z"))
    plan = compaction.build_table_plan(
        "orders", [tomb_row("O1"), tomb_row("O2", ts="2026-11-10T00:40:00Z")], sources
    )
    report = compaction.verify_batch({"orders": plan}, {"orders": 13}, sources)
    assert report["ok"] is True
    assert report["decision"] == "pass_all_tables"
    assert report["totals"] == {
        "tombstone_before": 2, "compacting": 2, "retained": 0, "tombstone_after": 0,
    }
    names = [c["name"] for c in report["per_table"]["orders"]["checks"]]
    assert "墓碑计数守恒" in names and "压缩墓碑均在保留边界内" in names
    assert "各来源一致放行" in names and "压缩记录留档完整" in names


def test_verify_batch_aborts_if_any_table_violates_boundary():
    sources = make_sources(("binlog", "2026-11-10T00:41:00Z"))
    # 墓碑 00:42 > 边界 00:41：计划本身会判保留，因此这里手工构造越界 decision 验证校验兜底
    plan = compaction.build_table_plan("orders", [tomb_row("O1")], sources)
    # 强制把一个保留项改成「可压缩」模拟数据被污染，校验必须抓出来
    plan.decisions[0].eligible = True
    report = compaction.verify_batch({"orders": plan}, {"orders": 13}, sources)
    assert report["ok"] is False
    assert report["decision"] == "abort_keep_previous"
    assert any("保留边界" in f for f in report["per_table"]["orders"]["failures"])


def test_verify_batch_requires_unanimous_release_votes():
    sources = make_sources(("binlog", "2026-11-10T00:45:00Z"),
                           ("mq", "2026-11-10T00:40:00Z"))
    plan = compaction.build_table_plan("orders", [tomb_row("O1", ts="2026-11-10T00:42:00Z")], sources)
    plan.decisions[0].eligible = True  # 人为制造分裂投票却被标可压缩
    report = compaction.verify_batch({"orders": plan}, {"orders": 1}, sources)
    assert report["ok"] is False
    assert any("一致放行" in f for f in report["per_table"]["orders"]["failures"])
