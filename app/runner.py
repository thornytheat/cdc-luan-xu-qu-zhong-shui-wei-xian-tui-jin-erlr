"""编排：把 6 个模块串成一次运行（纯函数部分可单测，不依赖数据库）。"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import archive, compaction, merge, schema_gate, settings, snapshot, store, watermark
from .settings import KIND_LABELS
from .tombstone import KeyState, evaluate

EVENT_COUNTERS = (
    "events",
    "processed",
    "applied_insert",
    "applied_update",
    "applied_delete",
    "resurrected",
    "deduped_silent",
    "conflicts",
    "superseded",
    "ignored_after_tombstone",
    "blocked_replay",
    "late",
    "keys",
    "alive_rows",
    "tombstones",
)


class RunError(ValueError):
    """可预期的运行错误（路由转成 4xx）。"""


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"R{stamp}-{uuid.uuid4().hex[:4].upper()}"


def validate_tables(tables: list[str]) -> list[str]:
    known = set(settings.TABLE_NAMES)
    unknown = [name for name in tables if name not in known]
    if unknown:
        raise RunError(
            f"未知的表：{', '.join(unknown)}；可选 {', '.join(settings.TABLE_NAMES)}"
        )
    ordered = [name for name in settings.TABLE_NAMES if name in set(tables)]
    if not ordered:
        raise RunError("至少选择一张表再执行合并")
    return ordered


def analyze(
    tables: list[str],
    events_by_table: dict[str, list[archive.Event]],
    base_doc: dict,
    target_doc: dict,
    conn=None,
) -> dict:
    """一次完整的合并 + 水位线 + 门禁分析。

    传入 conn 时走带压缩守卫的合并（重放拦截 / 当前墓碑过滤）；
    不传 conn 时保持纯函数路径，供既有单测直接使用。
    """
    if conn is not None:
        return analyze_guarded(conn, tables, events_by_table, base_doc, target_doc)
    late_by_table = {table: watermark.detect_late(events_by_table[table]) for table in tables}
    merged = merge.merge_tables({t: events_by_table[t] for t in tables}, late_by_table)

    table_marks: list[watermark.TableWatermark] = []
    payloads: dict[str, dict] = {}
    conflicts: list[dict] = []
    late_events: list[dict] = []
    changelog: list[dict] = []
    trajectory: dict[str, list[dict]] = {}
    counters = {key: 0 for key in EVENT_COUNTERS}

    for table in tables:
        result = merged[table]
        mark = watermark.watermark_for_table(table, result.key_max_ts)
        table_marks.append(mark)
        conflicts.extend(result.conflicts)
        late_events.extend(item.to_dict() for item in late_by_table[table])
        changelog.extend(result.changelog)
        trajectory[table] = watermark.trajectory(events_by_table[table], step=10)
        for key in EVENT_COUNTERS:
            counters[key] += int(result.counters.get(key, 0))
        payloads[table] = {
            "table": table,
            "label": settings.TABLE_LABELS.get(table, table),
            "rows": result.rows,
            "tombstones": result.tombstones,
            "row_count": len(result.rows),
            "tombstone_count": len(result.tombstones),
            "key_count": result.key_count,
            "counters": result.counters,
            "watermark": mark.to_dict(),
            "ignored_line_nos": result.ignored_line_nos,
        }

    gate_result = schema_gate.gate(base_doc, target_doc, tables)
    global_wm = watermark.combine(table_marks)
    result_payload = {
        "tables": payloads,
        "watermark": {
            **global_wm,
            "per_table": {mark.table: mark.to_dict() for mark in table_marks},
        },
        "counters": counters,
        "gate": gate_result.to_dict(),
        "conflicts": conflicts,
        "late_events": late_events,
        "changelog": changelog,
        "trajectory": trajectory,
    }
    result_payload["summary"] = build_summary(result_payload)
    return result_payload


def build_summary(result: dict) -> dict:
    counters = result["counters"]
    gate = result["gate"]
    return {
        "events": counters["events"],
        "conflicts": len(result["conflicts"]),
        "late": len(result["late_events"]),
        "ignored_after_tombstone": counters["ignored_after_tombstone"],
        "blocked_replay": counters.get("blocked_replay", 0),
        "deduped_silent": counters["deduped_silent"],
        "superseded": counters["superseded"],
        "alive_rows": counters["alive_rows"],
        "tombstones": counters["tombstones"],
        "keys": counters["keys"],
        "watermark": result["watermark"]["watermark"],
        "open_keys": result["watermark"]["open_keys"],
        "gate_verdict": gate["verdict"],
        "gate_counts": gate["counts"],
    }


def load_schemas(schema_version: int, schemas_dir: Path | None = None) -> tuple[dict, dict]:
    """基线固定为 v1，目标为所选版本（选 v1 即「无变更」）。"""
    schemas_dir = schemas_dir or settings.SCHEMAS_DIR
    base = schema_gate.load_schema(schemas_dir / "v1.json")
    if int(schema_version) == 1:
        return base, base
    target = schema_gate.load_schema(schemas_dir / f"v{schema_version}.json")
    return base, target


def execute(
    conn,
    tables: list[str],
    schema_version: int = 2,
    events_by_table: dict[str, list[archive.Event]] | None = None,
) -> dict:
    """读归档事件 → 分析 → 落库；返回可直接渲染的运行记录。"""
    if schema_version not in (1, 2):
        raise RunError(f"schema 版本只支持 1 或 2，收到 {schema_version}")
    ordered = validate_tables(tables)

    if events_by_table is None:
        events_by_table = {table: store.fetch_events(conn, table) for table in ordered}
        missing = [table for table in ordered if not events_by_table[table]]
        if missing:
            raise RunError(f"表 {', '.join(missing)} 还没有归档事件，请先加载样例数据")

    base_doc, target_doc = load_schemas(schema_version)
    result = analyze(ordered, events_by_table, base_doc, target_doc, conn=conn)
    run = {
        "run_id": new_run_id(),
        "created_at": store.now_iso(),
        "tables": ordered,
        "schema_version": int(schema_version),
        "summary": result["summary"],
        "result": result,
    }
    store.save_run(conn, run)
    return run


# --- 模块 7：压缩守卫合并 --------------------------------------------------


def merge_with_guards(conn, table: str, events: list[archive.Event]) -> merge.MergeResult:
    """单表合并 + 墓碑压缩守卫。

    在 merge.merge_table 的同一套版本规则之外增加两道守卫：
    1. 重放拦截：事件命中已压缩墓碑（compacted_tombstones）时，
       version <= 墓碑版本 → 拦截留档（blocked_replays），不进状态机、绝不复活；
       严格更高版本 → 允许走正常合并（可复活），压缩记录仍在归档可追溯。
    2. 当前墓碑过滤：合并结束后把已压缩键从「当前墓碑」里剔除，
       使快照/导出口径只含仍须保留的墓碑；归档墓碑在压缩页/归档表查看。

    计数器新增 blocked_replay（与 ignored_after_tombstone 分开，
    前者针对已压缩墓碑的补传，后者针对未压缩墓碑后的低版本事件）。
    """
    compacted = store.fetch_compacted_map(conn, table)
    # 压缩切换瞬间的归档基线：只有晚于基线到达的事件才算「压缩后补传重放」，
    # 压缩前已归档的历史事件（含删除事件本身）仍走正常合并口径。
    baseline = store.guard_baseline(conn, table)
    baseline_line = baseline["max_line_no"] if baseline else 0
    late_lines = {mark.line_no for mark in watermark.detect_late(events)}
    result = merge.MergeResult(table=table)
    counters = {
        "events": len(events),
        "applied_insert": 0,
        "applied_update": 0,
        "applied_delete": 0,
        "resurrected": 0,
        "deduped_silent": 0,
        "conflicts": 0,
        "superseded": 0,
        "ignored_after_tombstone": 0,
        "blocked_replay": 0,
        "late": 0,
    }
    counters["late"] = len(late_lines)
    states: dict[str, KeyState] = {}
    seq = 0

    for event in events:
        seq += 1
        # 守卫 1：已压缩墓碑的补传重放（仅针对压缩基线之后新到达的事件）
        compacted_row = compacted.get(event.pk)
        is_post_compaction = event.line_no > baseline_line
        if compacted_row is not None and is_post_compaction:
            verdict = compaction.classify_replay(event, compacted_row)
            if verdict == "blocked_replay":
                reason = compaction.replay_reason(event, compacted_row)
                store.record_blocked_replay(conn, event, compacted_row, reason)
                counters["blocked_replay"] += 1
                result.ignored_line_nos.append(event.line_no)
                result.changelog.append(
                    {
                        "seq": seq,
                        "table": table,
                        "line_no": event.line_no,
                        "pk": event.pk,
                        "version": event.version,
                        "op": event.op,
                        "ts": event.ts_iso,
                        "schema_version": event.schema_version,
                        "kind": "blocked_replay",
                        "kind_label": KIND_LABELS.get("blocked_replay", "压缩后重放拦截"),
                        "late": event.line_no in late_lines,
                        "reason": reason,
                    }
                )
                continue
            # 严格更高版本：放行到正常状态机（可能复活），归档记录保留

        state = states.get(event.pk)
        outcome = evaluate(state, event)
        is_late = event.line_no in late_lines
        counters[merge.COUNTER_KEYS[outcome.kind]] += 1

        if outcome.kind == "conflict":
            result.conflicts.append(
                {
                    "table": table,
                    "pk": event.pk,
                    "version": event.version,
                    "diff": outcome.extra.get("diff", []),
                    "first": {
                        "line_no": state.line_no,
                        "ts": merge._fmt(state.ts),
                        "op": state.op,
                        "schema_version": state.schema_version,
                        "after": state.after,
                    },
                    "second": {
                        "line_no": event.line_no,
                        "ts": event.ts_iso,
                        "op": event.op,
                        "schema_version": event.schema_version,
                        "after": event.after,
                    },
                    "reason": outcome.reason,
                }
            )
        elif outcome.kind == "ignored_after_tombstone":
            result.ignored_line_nos.append(event.line_no)
        elif outcome.kind != "superseded":
            if outcome.kind in ("apply_insert", "apply_update", "apply_delete", "resurrect"):
                if state is None:
                    state = KeyState(
                        pk=event.pk,
                        version=event.version,
                        op=event.op,
                        after=dict(event.after),
                        deleted=event.is_delete,
                        line_no=event.line_no,
                        ts=event.ts,
                        schema_version=event.schema_version,
                        tombstone_version=event.version if event.is_delete else None,
                        tombstone_line_no=event.line_no if event.is_delete else None,
                        applied=1,
                        first_line_no=event.line_no,
                    )
                    states[event.pk] = state
                else:
                    if event.is_delete:
                        state.tombstone_version = event.version
                        state.tombstone_line_no = event.line_no
                    state.version = event.version
                    state.op = event.op
                    state.after = dict(event.after)
                    state.deleted = event.is_delete
                    state.line_no = event.line_no
                    state.ts = event.ts
                    state.schema_version = event.schema_version
                    state.applied += 1

        if outcome.kind != "ignored_after_tombstone":
            current = result.key_max_ts.get(event.pk)
            if current is None or event.ts > current:
                result.key_max_ts[event.pk] = event.ts

        entry = {
            "seq": seq,
            "table": table,
            "line_no": event.line_no,
            "pk": event.pk,
            "version": event.version,
            "op": event.op,
            "ts": event.ts_iso,
            "schema_version": event.schema_version,
            "kind": outcome.kind,
            "kind_label": KIND_LABELS.get(outcome.kind, outcome.kind),
            "late": is_late,
            "reason": outcome.reason,
        }
        if outcome.kind in ("apply_insert", "apply_update", "apply_delete", "resurrect"):
            entry["after"] = event.after
        if outcome.kind == "conflict":
            entry["diff"] = outcome.extra.get("diff", [])
        result.changelog.append(entry)

    for pk in sorted(states):
        state = states[pk]
        row = {
            "pk": pk,
            "version": state.version,
            "op": state.op,
            "ts": merge._fmt(state.ts),
            "line_no": state.line_no,
            "schema_version": state.schema_version,
            "applied": state.applied,
        }
        if state.deleted:
            row.update(
                {
                    "deleted": True,
                    "tombstone_version": state.tombstone_version,
                    "tombstone_line_no": state.tombstone_line_no,
                }
            )
            result.tombstones.append(row)
        else:
            row["after"] = state.after
            if state.tombstone_version is not None:
                row["resurrected_from"] = {
                    "tombstone_version": state.tombstone_version,
                    "tombstone_line_no": state.tombstone_line_no,
                }
            result.rows.append(row)

    # 守卫 2：已压缩墓碑不再计入「当前墓碑」（归档留底在 compacted_tombstones）
    if compacted:
        result.tombstones = [row for row in result.tombstones if row["pk"] not in compacted]

    counters["processed"] = (
        counters["events"]
        - counters["ignored_after_tombstone"]
        - counters["blocked_replay"]
    )
    counters["keys"] = len(states)
    counters["alive_rows"] = len(result.rows)
    counters["tombstones"] = len(result.tombstones)
    result.counters = counters
    return result


def analyze_guarded(
    conn,
    tables: list[str],
    events_by_table: dict[str, list[archive.Event]],
    base_doc: dict,
    target_doc: dict,
) -> dict:
    """与 analyze 同构，但合并走压缩守卫版本（含重放拦截与当前墓碑过滤）。"""
    late_by_table = {table: watermark.detect_late(events_by_table[table]) for table in tables}
    table_marks: list[watermark.TableWatermark] = []
    payloads: dict[str, dict] = {}
    conflicts: list[dict] = []
    late_events: list[dict] = []
    changelog: list[dict] = []
    trajectory: dict[str, list[dict]] = {}
    counters = {key: 0 for key in EVENT_COUNTERS}

    for table in tables:
        result = merge_with_guards(conn, table, events_by_table[table])
        mark = watermark.watermark_for_table(table, result.key_max_ts)
        table_marks.append(mark)
        conflicts.extend(result.conflicts)
        late_events.extend(item.to_dict() for item in late_by_table[table])
        changelog.extend(result.changelog)
        trajectory[table] = watermark.trajectory(events_by_table[table], step=10)
        for key in counters:
            counters[key] += int(result.counters.get(key, 0))
        payloads[table] = {
            "table": table,
            "label": settings.TABLE_LABELS.get(table, table),
            "rows": result.rows,
            "tombstones": result.tombstones,
            "row_count": len(result.rows),
            "tombstone_count": len(result.tombstones),
            "key_count": len(result.rows) + len(result.tombstones),
            "counters": result.counters,
            "watermark": mark.to_dict(),
            "ignored_line_nos": result.ignored_line_nos,
        }

    gate_result = schema_gate.gate(base_doc, target_doc, tables)
    global_wm = watermark.combine(table_marks)
    result_payload = {
        "tables": payloads,
        "watermark": {
            **global_wm,
            "per_table": {mark.table: mark.to_dict() for mark in table_marks},
        },
        "counters": counters,
        "gate": gate_result.to_dict(),
        "conflicts": conflicts,
        "late_events": late_events,
        "changelog": changelog,
        "trajectory": trajectory,
    }
    result_payload["summary"] = build_summary(result_payload)
    return result_payload


def snapshot_for_run(run: dict) -> dict:
    return snapshot.build_snapshot(run)
