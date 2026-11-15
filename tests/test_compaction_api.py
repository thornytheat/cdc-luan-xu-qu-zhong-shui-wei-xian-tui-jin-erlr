"""模块 7 端到端：来源边界 → 压缩作业 → 重放拦截 → 快照/页面。"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app import store
from app.archive import Event
from app.retention import SourceWatermark


SOURCES_45 = [
    ("binlog", "2026-11-10T00:45:00Z"),
    ("mq", "2026-11-10T00:46:00Z"),
    ("backfill", "2026-11-10T00:50:00Z"),
]
SOURCES_40 = [("binlog", "2026-11-10T00:40:00Z"), ("mq", "2026-11-10T00:41:00Z")]


def register(client, pairs):
    for name, mark in pairs:
        resp = client.post("/api/retention/sources", json={"source": name, "watermark": mark})
        assert resp.status_code == 200


def make_event(line_no, pk, version, minute, op="u", table="orders"):
    return Event(
        table=table, line_no=line_no, pk=pk, version=version,
        ts=datetime(2026, 11, 10, 0, minute, tzinfo=timezone.utc),
        op=op, after={"order_id": pk, "amount": version},
        schema_version=1, source=f"{table}.jsonl",
    )


# --- 来源边界 --------------------------------------------------------------


def test_no_source_rejects_compaction(client):
    resp = client.post("/api/compaction/jobs", json={"tables": ["orders"]})
    assert resp.status_code == 400
    assert "来源" in resp.json()["detail"]


def test_source_rollback_and_bad_timestamp_are_400(client):
    client.post("/api/retention/sources", json={"source": "binlog", "watermark": SOURCES_45[0][1]})
    rollback = client.post(
        "/api/retention/sources", json={"source": "binlog", "watermark": "2026-11-10T00:01:00Z"}
    )
    assert rollback.status_code == 400 and "回退" in rollback.json()["detail"]
    bad = client.post(
        "/api/retention/sources", json={"source": "x", "watermark": "nonsense"}
    )
    assert bad.status_code == 400


def test_joint_boundary_uses_min_source(client):
    register(client, SOURCES_45)
    view = client.get("/api/retention/sources").json()
    assert view["boundary"] == "2026-11-10T00:45:00Z"
    assert view["boundary_source"] == "binlog"
    assert view["source_count"] == 3


# --- 压缩作业：边界保留与压缩 ----------------------------------------------


def test_tombstones_above_boundary_are_retained(client):
    register(client, SOURCES_40)  # 共同边界 00:40 < 墓碑 ts 00:42
    job = client.post(
        "/api/compaction/jobs", json={"tables": ["orders", "inventory"]}
    ).json()
    assert job["status"] == "completed"
    assert job["verification"]["totals"]["compacting"] == 0
    assert job["verification"]["totals"]["retained"] == 2
    # 现网墓碑仍在
    run = client.post(
        "/api/merge", json={"tables": ["orders", "inventory"], "schema_version": 2}
    ).json()
    assert run["summary"]["tombstones"] == 2
    assert client.get("/api/compaction/archived").json()["count"] == 0


def test_compaction_archives_tombstones_and_shrinks_snapshot(client):
    register(client, SOURCES_45)
    job = client.post(
        "/api/compaction/jobs", json={"tables": ["orders", "inventory"]}
    ).json()
    assert job["status"] == "completed"
    assert job["verification"]["decision"] == "pass_all_tables"
    totals = job["verification"]["totals"]
    assert totals == {"tombstone_before": 2, "compacting": 2, "retained": 0,
                      "tombstone_after": 0}
    # 归档留底
    archived = client.get("/api/compaction/archived").json()["archived"]
    assert {(r["table"], r["pk"]) for r in archived} == {
        ("orders", "O9002"), ("inventory", "I9002")
    }
    # 重新合并：当前墓碑为 0，但存活行不受影响
    run = client.post(
        "/api/merge", json={"tables": ["orders", "inventory"], "schema_version": 2}
    ).json()
    assert run["summary"]["tombstones"] == 0
    assert run["summary"]["alive_rows"] == 26  # 13 + 13，压缩不碰存活行
    # 快照：当前墓碑清空，压缩段留底
    snap = client.get(f"/api/runs/{run['run_id']}/snapshot").json()
    assert sum(t["tombstone_count"] for t in snap["tables"].values()) == 0
    assert snap["compaction"]["archived_count"] == 2


def test_rerun_same_batch_does_not_double_count(client):
    register(client, SOURCES_45)
    first = client.post(
        "/api/compaction/jobs", json={"tables": ["orders", "inventory"]}
    ).json()
    assert first["verification"]["totals"]["compacting"] == 2
    second = client.post(
        "/api/compaction/jobs", json={"tables": ["orders", "inventory"]}
    ).json()
    assert second["verification"]["totals"]["compacting"] == 0
    assert client.get("/api/compaction/archived").json()["count"] == 2
    # 作业历史里两次都完成，各自清理次数独立、归档不重复
    jobs = client.get("/api/compaction/jobs").json()["jobs"]
    assert len(jobs) == 2
    assert all(j["status"] == "completed" for j in jobs)


# --- 重放拦截：不复活 + 留档 + 幂等 ----------------------------------------


def test_post_compaction_low_version_replay_is_blocked_and_kept(client):
    register(client, SOURCES_45)
    client.post("/api/compaction/jobs", json={"tables": ["orders"]})
    conn = store.connect()
    try:
        # 基线行号 60 之后补传：低版本 v2、等版本 v3 拦截，高版本 v4 复活
        store.archive_events(conn, [
            make_event(61, "O9002", 2, 30, "u"),
            make_event(62, "O9002", 3, 31, "c"),
            make_event(63, "O9002", 4, 55, "c"),
        ])
    finally:
        conn.close()

    run = client.post("/api/merge", json={"tables": ["orders"], "schema_version": 2}).json()
    assert run["summary"]["blocked_replay"] == 2
    detail = client.get(f"/api/runs/{run['run_id']}").json()
    assert detail["tables"]["orders"]["counters"]["resurrected"] == 1

    blocked = client.get("/api/compaction/blocked-replays?table=orders").json()
    assert sorted((r["line_no"], r["event_version"]) for r in blocked["blocked_replays"]) == [
        (61, 2), (62, 3)
    ]
    # 重跑合并：唯一键保证拦截不重复计数
    run2 = client.post("/api/merge", json={"tables": ["orders"], "schema_version": 2}).json()
    assert run2["summary"]["blocked_replay"] == 2
    assert client.get("/api/compaction/blocked-replays?table=orders").json()["count"] == 2
    # 拦截记录进快照
    snap = client.get(f"/api/runs/{run2['run_id']}/snapshot").json()
    assert snap["compaction"]["blocked_count"] == 2


def test_historical_events_are_not_falsely_blocked(client):
    """压缩前已归档的历史事件（含删除事件本身）走正常合并，不算重放拦截。"""
    register(client, SOURCES_45)
    client.post("/api/compaction/jobs", json={"tables": ["orders", "inventory"]})
    run = client.post(
        "/api/merge", json={"tables": ["orders", "inventory"], "schema_version": 2}
    ).json()
    assert run["summary"]["blocked_replay"] == 0
    assert client.get("/api/compaction/blocked-replays").json()["count"] == 0


# --- 断点续跑与失败保留旧版 ------------------------------------------------


def test_resume_running_job_skips_completed_tables(client):
    register(client, SOURCES_45)
    # 手工造一个只完成 orders 的 running 作业
    from app import compaction_runner
    from app.retention import boundary_views

    conn = store.connect()
    try:
        sources = compaction_runner.load_sources(conn)
        jobrow = {
            "job_id": "TC-RESUME-1", "created_at": store.now_iso(),
            "updated_at": store.now_iso(), "status": "running",
            "tables": ["orders", "inventory"], "boundary": boundary_views(sources),
            "completed": [], "plan": {}, "verification": {}, "summary": {}, "error": "",
        }
        store.save_compaction_job(conn, jobrow)
        from app import compaction
        tombstones, _ = compaction_runner.current_tombstones(conn, "orders")
        plan = compaction.build_table_plan("orders", tombstones, sources)
        store.insert_compacted_tombstones(
            conn, jobrow["job_id"], "orders",
            [d.to_dict() for d in plan.eligible],
            boundary_views(sources)["boundary"], {r["pk"]: r for r in tombstones},
        )
        store.save_compaction_baselines(
            conn, jobrow["job_id"],
            compaction_runner.archive_baseline(conn, ["orders", "inventory"]),
        )
        jobrow["completed"] = ["orders"]
        store.save_compaction_job(conn, jobrow)
    finally:
        conn.close()

    # 同表清单再次发起：应续跑而不是新建
    resumed = client.post(
        "/api/compaction/jobs", json={"tables": ["orders", "inventory"]}
    ).json()
    assert resumed["job_id"] == "TC-RESUME-1"
    assert resumed["status"] == "completed"
    assert resumed["completed"] == ["orders", "inventory"]
    assert client.get("/api/compaction/archived").json()["count"] == 2
    # 显式 resume 端点对已完成作业幂等
    again = client.post("/api/compaction/jobs/TC-RESUME-1/resume", json={}).json()
    assert again["status"] == "completed"


def test_failed_job_keeps_previous_version_and_allows_retry(conn):
    from app import compaction, compaction_runner

    store.upsert_source_watermark(conn, "binlog", "2026-11-10T00:45:00Z")
    store.upsert_source_watermark(conn, "mq", "2026-11-10T00:46:00Z")
    orig = compaction.build_table_plan
    calls = {"n": 0}

    def flaky(table, tombstones, sources):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("模拟中断")
        return orig(table, tombstones, sources)

    compaction.build_table_plan = flaky
    try:
        with pytest.raises(RuntimeError):
            compaction_runner.start_job(conn, ["orders", "inventory"])
    finally:
        compaction.build_table_plan = orig

    job = store.list_compaction_jobs(conn)[0]
    assert job["status"] == "failed"
    assert store.compacted_count(conn) == 0          # 现网未切换
    assert conn.execute("SELECT COUNT(*) n FROM compaction_baselines").fetchone()["n"] == 0

    retry = compaction_runner.start_job(conn, ["orders", "inventory"])
    assert retry["status"] == "completed"
    assert store.compacted_count(conn) == 2


# --- 页面 ------------------------------------------------------------------


def test_compaction_pages_render(client):
    register(client, SOURCES_45)
    client.post("/api/compaction/jobs", json={"tables": ["orders", "inventory"]})
    workbench = client.get("/compaction")
    assert workbench.status_code == 200
    assert "墓碑压缩" in workbench.text and "各来源保留边界" in workbench.text
    job_id = client.get("/api/compaction/jobs").json()["jobs"][0]["job_id"]
    result = client.get(f"/compaction/{job_id}")
    assert result.status_code == 200
    text = result.text
    assert "清理前后对照" in text
    assert "整批影子校验" in text
    assert "逐墓碑判定与各来源投票" in text
    assert "拦截记录" in text


def test_unknown_job_returns_404(client):
    assert client.get("/api/compaction/jobs/TC-ghost").status_code == 404
    assert client.post("/api/compaction/jobs/TC-ghost/resume", json={}).status_code == 404
    page = client.get("/compaction/TC-ghost")
    assert page.status_code == 200
    assert "没有找到压缩作业" in page.text


def test_commit_phase_failure_rolls_back_archive_and_baseline(conn):
    """切换事务内任何一步失败，归档与基线都必须整体回滚，现网保持旧版。"""
    import sqlite3

    from app import compaction_runner, store as store_mod

    store.upsert_source_watermark(conn, "binlog", "2026-11-10T00:45:00Z")
    store.upsert_source_watermark(conn, "mq", "2026-11-10T00:46:00Z")
    original = store_mod.save_compaction_job

    def fail_on_complete(connection, job):
        if job.get("status") == "completed":
            raise sqlite3.OperationalError("模拟提交阶段磁盘故障")
        return original(connection, job)

    store_mod.save_compaction_job = fail_on_complete
    try:
        with pytest.raises(sqlite3.OperationalError):
            compaction_runner.start_job(conn, ["orders", "inventory"])
    finally:
        store_mod.save_compaction_job = original

    assert store.compacted_count(conn) == 0
    assert conn.execute("SELECT COUNT(*) n FROM compaction_baselines").fetchone()["n"] == 0
    # 现网墓碑仍在，且可重试成功
    tombstones, _ = compaction_runner.current_tombstones(conn, "orders")
    assert len(tombstones) == 1
    retry = compaction_runner.start_job(conn, ["orders", "inventory"])
    assert retry["status"] == "completed"
    assert store.compacted_count(conn) == 2


def test_health_reports_compaction_counters(client):
    register(client, SOURCES_45)
    client.post("/api/compaction/jobs", json={"tables": ["orders"]})
    body = client.get("/health").json()
    assert body["compaction_jobs"] == 1
    assert body["compacted_tombstones"] == 1
    assert "blocked_replays" in body
