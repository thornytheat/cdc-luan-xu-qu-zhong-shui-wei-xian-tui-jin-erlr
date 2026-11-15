"""模块 6 · 快照导出：把一次运行的结果拼成可下载 JSON，并给出页面预览。

快照结构（对外契约）：
{
  "export":   {app, run_id, generated_at, tables, schema_version},
  "watermark":{watermark, open_keys, closed_keys, keys, per_table},
  "counters": {...全局计数...},
  "tables":   {表: {rows:[...], tombstones:[...], columns:[...]}},
  "gate":     {verdict, counts, findings, tables},
  "changelog":[...全部逐条事件结论...],
  "late_events":[...迟到位置...]
}
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

PREVIEW_ROWS = 6


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def column_order(rows: list[dict]) -> list[str]:
    """按首次出现顺序合并所有行的字段，删除态没有 after 时返回空列表。"""
    order: list[str] = []
    for row in rows:
        for key in (row.get("after") or {}):
            if key not in order:
                order.append(key)
    return order


def build_snapshot(run: dict, conn=None) -> dict:
    """run 为 runner 产出的运行记录（含 result），这里只做结构化与精简。

    传入 conn 时附带模块 7 的压缩归档与拦截记录：压缩后的墓碑不再出现在
    「当前墓碑」里（导出体积随之下降），但完整留底在 compaction 段可追溯。
    """
    result = run["result"]
    tables: dict[str, dict] = {}
    for table, payload in result["tables"].items():
        rows = payload["rows"]
        tables[table] = {
            "label": payload.get("label", table),
            "row_count": payload["row_count"],
            "tombstone_count": payload["tombstone_count"],
            "columns": column_order(rows),
            "rows": rows,
            "tombstones": payload["tombstones"],
            "counters": payload["counters"],
            "watermark": payload["watermark"],
        }
    snapshot_doc = {
        "export": {
            "app": "cdcwater",
            "app_name": "CDC 乱序去重与水位线推进",
            "run_id": run["run_id"],
            "created_at": run["created_at"],
            "generated_at": _now(),
            "tables": run["tables"],
            "schema_version": run["schema_version"],
            "schema_pair": [result["gate"]["base_version"], result["gate"]["target_version"]],
        },
        "watermark": result["watermark"],
        "counters": result["counters"],
        "tables": tables,
        "gate": result["gate"],
        "changelog": result["changelog"],
        "late_events": result["late_events"],
        "conflicts": result["conflicts"],
    }
    if conn is not None:
        from . import store

        compacted = store.fetch_compacted_tombstones(conn)
        blocked = store.fetch_blocked_replays(conn)
        snapshot_doc["compaction"] = {
            "note": "已压缩墓碑不进当前墓碑与表导出，仅在此段留底；拦截的低版本重放不复活",
            "archived_tombstones": compacted,
            "archived_count": len(compacted),
            "blocked_replays": blocked,
            "blocked_count": len(blocked),
        }
    return snapshot_doc


def dumps(snapshot: dict) -> str:
    return json.dumps(snapshot, ensure_ascii=False, indent=2)


def preview(snapshot: dict, limit: int = PREVIEW_ROWS) -> dict:
    """页面预览：每表取前 limit 行 + 列头 + 墓碑前若干条，避免页面贴大 JSON。"""
    out: dict[str, dict] = {}
    for table, payload in snapshot["tables"].items():
        rows = payload["rows"][:limit]
        tombstones = payload["tombstones"][:limit]
        out[table] = {
            "label": payload["label"],
            "columns": payload["columns"],
            "rows": [
                {
                    "pk": row["pk"],
                    "version": row["version"],
                    "ts": row["ts"],
                    "line_no": row["line_no"],
                    "cells": [row.get("after", {}).get(col) for col in payload["columns"]],
                }
                for row in rows
            ],
            "tombstones": [
                {
                    "pk": row["pk"],
                    "tombstone_version": row.get("tombstone_version"),
                    "line_no": row.get("tombstone_line_no"),
                    "ts": row["ts"],
                }
                for row in tombstones
            ],
            "row_count": payload["row_count"],
            "tombstone_count": payload["tombstone_count"],
            "shown": len(rows),
        }
    return out


def snapshot_size(snapshot: dict) -> int:
    return len(dumps(snapshot).encode("utf-8"))
