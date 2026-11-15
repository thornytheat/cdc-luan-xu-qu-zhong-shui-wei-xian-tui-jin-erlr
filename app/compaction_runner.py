"""模块 7c · 墓碑压缩编排：作业状态机、断点续跑、整批校验后原子切换。

作业生命周期（compaction_jobs.status）：

    running ──整批校验通过 + 切换成功──▶ completed
       │
       └──任一表校验失败 / 出错──▶ failed（旧版完整保留，可修正后重开作业）

关键保证：
- 逐表处理，每张表先在内存里做「影子计划 + 影子校验」，不碰现网；
  全部表校验通过后才在一个事务里把墓碑搬入归档（切换现网）。
- 中断续跑：running 作业按已完成表清单跳过已完成表，从下一张表继续；
  completed 作业直接返回历史结果；切换前失败不会留下半截归档。
- 幂等重跑：归档表主键 (table, pk) 与拦截表唯一键 (source, line_no) 去重，
  同批重跑既不会重复增加清理次数，也不会把仍须保留的墓碑误归档。
- 成功前旧版保留：任何失败路径都不写入 compacted_tombstones，
  当前墓碑/快照口径与作业开始前完全一致。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from . import compaction, merge, retention, runner, store, watermark
from .retention import RetentionError, SourceWatermark

JOB_STATUSES = ("running", "completed", "failed")


class JobError(ValueError):
    """压缩作业的可预期错误（路由转 4xx）。"""


def new_job_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"TC{stamp}-{uuid.uuid4().hex[:4].upper()}"


def load_sources(conn) -> dict[str, SourceWatermark]:
    """从 SQLite 读取已登记来源水位线，转成纯函数层使用的对象。"""
    sources: dict[str, SourceWatermark] = {}
    for row in store.fetch_source_watermarks(conn):
        sources[row["source"]] = SourceWatermark(
            source=row["source"],
            watermark=retention_source_ts(row["watermark"]),
            note=row.get("note", ""),
        )
    return sources


def retention_source_ts(raw: str):
    from .archive import parse_ts

    return parse_ts(raw)


def current_tombstones(conn, table: str) -> tuple[list[dict], int]:
    """重新合并一张表，返回（当前墓碑行, 存活行数）。

    已压缩墓碑在归档表里，不会出现在合并结果中（合并输入仍是原始归档事件，
    但带压缩守卫，见 runner.merge_with_guards）。
    """
    events = store.fetch_events(conn, table)
    result = runner.merge_with_guards(conn, table, events)
    return result.tombstones, len(result.rows)


def archive_baseline(conn, tables: list[str]) -> dict[str, dict]:
    """压缩切换瞬间的归档基线，用于区分「历史事件」与「压缩后补传」。

    每张表记录当时最大归档行号与事件数；重放守卫只拦截基线之后到达的事件，
    压缩前已归档的历史事件（含删除事件本身）仍走正常合并，不会被误计拦截。
    """
    baseline: dict[str, dict] = {}
    for table in tables:
        events = store.fetch_events(conn, table)
        baseline[table] = {
            "max_line_no": max((event.line_no for event in events), default=0),
            "event_count": len(events),
        }
    return baseline


def start_job(conn, tables: list[str], sources: dict[str, SourceWatermark] | None = None) -> dict:
    """开启（或接续）一个压缩作业。

    - 没有任何来源水位线 → 显式拒绝（保守，不允许无边界压缩）。
    - 已存在同表清单的 running 作业 → 直接续跑，不新建。
    - 已存在同表清单的 completed 作业且边界未推进 → 返回上次结果（幂等）。
    """
    ordered = runner.validate_tables(tables)
    if sources is None:
        sources = load_sources(conn)
    if not sources:
        raise JobError("尚未登记任何来源的安全水位线，无法确定共同保留边界，拒绝压缩")

    boundary_view = retention.boundary_views(sources)

    existing = store.find_resumable_job(conn, ordered)
    if existing is not None:
        return run_job(conn, existing["job_id"])

    job = {
        "job_id": new_job_id(),
        "created_at": store.now_iso(),
        "updated_at": store.now_iso(),
        "status": "running",
        "tables": ordered,
        "boundary": boundary_view,
        "completed": [],
        "plan": {},
        "verification": {},
        "summary": {},
        "error": "",
    }
    store.save_compaction_job(conn, job)
    return run_job(conn, job["job_id"])


def run_job(conn, job_id: str) -> dict:
    """执行/续跑作业；全程异常都会把作业置 failed 并保留旧版。"""
    job = store.get_compaction_job(conn, job_id)
    if job is None:
        raise JobError(f"压缩作业不存在：{job_id}")
    if job["status"] == "completed":
        return job
    try:
        sources = load_sources(conn)
        if not sources:
            raise JobError("来源水位线已被清空，无法续跑压缩作业")
        boundary = retention.joint_boundary(sources)
        if boundary is None:
            raise JobError("共同保留边界为空，拒绝压缩")
        boundary_iso = boundary.strftime("%Y-%m-%dT%H:%M:%SZ")
        boundary_view = retention.boundary_views(sources)

        plans: dict[str, compaction.TablePlan] = {}
        alive_by_table: dict[str, int] = {}
        before_counts: dict[str, int] = {}
        payloads_by_table: dict[str, dict[str, dict]] = {}

        done = set(job["completed"])
        for table in job["tables"]:
            tombstones, alive = current_tombstones(conn, table)
            alive_by_table[table] = alive
            before_counts[table] = len(tombstones)
            payloads_by_table[table] = {row["pk"]: row for row in tombstones}
            # 已压缩归档中的同键墓碑不参与本次计划（重跑不重复计数）
            compacted = store.fetch_compacted_map(conn, table)
            fresh = [row for row in tombstones if row["pk"] not in compacted]
            plans[table] = compaction.build_table_plan(table, fresh, sources)

        # 整批影子校验：所有表都通过才允许切换
        verification = compaction.verify_batch(plans, alive_by_table, sources)
        job["plan"] = {table: plan.to_dict() for table, plan in plans.items()}
        job["verification"] = verification
        if not verification["ok"]:
            raise JobError(
                "整批校验未通过，已放弃切换并保留旧版："
                + "；".join(
                    failure
                    for check in verification["per_table"].values()
                    for failure in check["failures"]
                )
            )

        # 全部表校验通过：单事务原子切换（墓碑搬入归档 + 记录归档基线），断点表同步推进
        updated = dict(job)
        updated["updated_at"] = store.now_iso()
        baselines = archive_baseline(conn, job["tables"])
        with conn:  # 原子提交：中途失败整体回滚，现网不变
            for table in job["tables"]:
                # 基线按表幂等写入：续跑时为上一段已完成表补齐，保证重放守卫行号口径完整
                store.save_compaction_baselines(conn, job_id, {table: baselines[table]})
                if table in done:
                    continue
                plan = plans[table]
                store.insert_compacted_tombstones(
                    conn,
                    job_id,
                    table,
                    [item.to_dict() for item in plan.eligible],
                    boundary_iso,
                    payloads_by_table[table],
                )
                if table not in updated["completed"]:
                    updated["completed"].append(table)
            updated["status"] = "completed"
            updated["summary"] = build_summary(
                conn, job["tables"], plans, before_counts, alive_by_table, boundary_view
            )
            store.save_compaction_job(conn, updated)
        return store.get_compaction_job(conn, job_id)
    except (JobError, RetentionError) as exc:
        mark_failed(conn, job, str(exc))
        raise
    except Exception as exc:  # 防御：任何意外都不许留下 running 假死作业
        mark_failed(conn, job, f"未预期错误：{exc}")
        raise


def mark_failed(conn, job: dict, message: str) -> None:
    job["status"] = "failed"
    job["error"] = message
    job["updated_at"] = store.now_iso()
    store.save_compaction_job(conn, job)


def build_summary(
    conn,
    tables: list[str],
    plans: dict[str, compaction.TablePlan],
    before_counts: dict[str, int],
    alive_by_table: dict[str, int],
    boundary_view: dict,
) -> dict:
    per_table = {}
    total_before = total_after = total_archived = 0
    for table in tables:
        archived = len(plans[table].eligible)
        # 归档总量含历史作业（重跑后累计口径），当前墓碑以重新合并结果为准
        archived_total = store.compacted_count(conn, table)
        before = before_counts[table]
        after = before - archived
        total_before += before
        total_after += after
        total_archived += archived
        per_table[table] = {
            "table": table,
            "tombstone_before": before,
            "compacted_this_job": archived,
            "retained": len(plans[table].retained),
            "tombstone_after": after,
            "archived_total": archived_total,
            "alive_rows": alive_by_table[table],
            "blocked_replays": store.blocked_replay_count(conn, table),
        }
    return {
        "boundary": boundary_view,
        "tables": per_table,
        "totals": {
            "tombstone_before": total_before,
            "compacted_this_job": total_archived,
            "tombstone_after": total_after,
            "archived_total": store.compacted_count(conn),
            "blocked_replays": store.blocked_replay_count(conn),
        },
    }


def job_detail(conn, job_id: str) -> dict:
    """结果页用：作业 + 归档明细 + 拦截记录 + 边界视图。"""
    job = store.get_compaction_job(conn, job_id)
    if job is None:
        raise JobError(f"压缩作业不存在：{job_id}")
    job["archived"] = store.fetch_compacted_tombstones(conn)
    job["blocked_replays"] = store.fetch_blocked_replays(conn)
    return job
