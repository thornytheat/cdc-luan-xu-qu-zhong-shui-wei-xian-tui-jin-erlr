"""SQLite 访问（薄封装）。

表：
- events                 归档事件；主键 (source, line_no) 保证重复启动/重复 seed 不会重复导入
- runs                   每次合并运行的元信息 + 完整结果 JSON
- meta                   seed 版本等键值
- source_watermarks      模块7：各来源安全水位线（共同保留边界 = min）
- compaction_jobs        模块7：墓碑压缩批次作业（断点续跑 / 状态机 / 结果留档）
- compacted_tombstones   模块7：已压缩墓碑归档（留底，快照/导出不再计当前墓碑）
- blocked_replays        模块7：压缩后补传、被拦截的低版本重放记录（留档，不复活）
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import settings
from .archive import Event, load_events_file, parse_ts

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  source         TEXT    NOT NULL,
  line_no        INTEGER NOT NULL,
  table_name     TEXT    NOT NULL,
  pk             TEXT    NOT NULL,
  version        INTEGER NOT NULL,
  ts             TEXT    NOT NULL,
  op             TEXT    NOT NULL,
  after_json     TEXT    NOT NULL,
  schema_version INTEGER NOT NULL,
  PRIMARY KEY (source, line_no)
);
CREATE INDEX IF NOT EXISTS idx_events_table ON events(table_name, line_no);

CREATE TABLE IF NOT EXISTS runs (
  run_id         TEXT PRIMARY KEY,
  created_at     TEXT NOT NULL,
  tables_json    TEXT NOT NULL,
  schema_version INTEGER NOT NULL,
  summary_json   TEXT NOT NULL,
  payload_json   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at DESC);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- 模块 7：墓碑压缩 -------------------------------------------------------
CREATE TABLE IF NOT EXISTS source_watermarks (
  source        TEXT PRIMARY KEY,
  watermark_ts  TEXT NOT NULL,
  note          TEXT NOT NULL DEFAULT '',
  updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS compaction_jobs (
  job_id          TEXT PRIMARY KEY,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  status          TEXT NOT NULL,            -- running/completed/failed
  tables_json     TEXT NOT NULL,
  boundary_json   TEXT NOT NULL,            -- 逐来源水位线 + 共同边界快照
  completed_json  TEXT NOT NULL DEFAULT '[]',
  plan_json       TEXT NOT NULL DEFAULT '{}',
  verification_json TEXT NOT NULL DEFAULT '{}',
  summary_json    TEXT NOT NULL DEFAULT '{}',
  error           TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS compacted_tombstones (
  job_id              TEXT NOT NULL,
  table_name          TEXT NOT NULL,
  pk                  TEXT NOT NULL,
  tombstone_version   INTEGER NOT NULL,
  tombstone_line_no   INTEGER NOT NULL,
  tombstone_ts        TEXT NOT NULL,
  boundary_ts         TEXT NOT NULL,
  compacted_at        TEXT NOT NULL,
  payload_json        TEXT NOT NULL,
  PRIMARY KEY (table_name, pk)
);
CREATE INDEX IF NOT EXISTS idx_compacted_job ON compacted_tombstones(job_id);

CREATE TABLE IF NOT EXISTS blocked_replays (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id              TEXT NOT NULL DEFAULT '',
  source              TEXT NOT NULL,
  line_no             INTEGER NOT NULL,
  table_name          TEXT NOT NULL,
  pk                  TEXT NOT NULL,
  event_version       INTEGER NOT NULL,
  event_ts            TEXT NOT NULL,
  op                  TEXT NOT NULL,
  tombstone_version   INTEGER NOT NULL,
  reason              TEXT NOT NULL,
  blocked_at          TEXT NOT NULL,
  UNIQUE (source, line_no)
);
CREATE INDEX IF NOT EXISTS idx_blocked_table ON blocked_replays(table_name, pk);

-- 压缩切换瞬间的归档基线（区分历史事件与压缩后补传重放）
CREATE TABLE IF NOT EXISTS compaction_baselines (
  job_id        TEXT NOT NULL,
  table_name    TEXT NOT NULL,
  max_line_no   INTEGER NOT NULL,
  event_count   INTEGER NOT NULL,
  created_at    TEXT NOT NULL,
  PRIMARY KEY (job_id, table_name)
);
"""


def connect() -> sqlite3.Connection:
    settings.ensure_data_dir()
    conn = sqlite3.connect(settings.db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --- meta ------------------------------------------------------------------


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT OR REPLACE INTO meta(key, value) VALUES(?, ?)", (key, value))
    conn.commit()


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


# --- events ----------------------------------------------------------------


def archive_events(conn: sqlite3.Connection, events: list[Event]) -> dict:
    """幂等归档：主键冲突一律忽略（重复启动/重复 seed 不产生脏数据）。"""
    inserted = 0
    for event in events:
        row = event.to_row()
        cursor = conn.execute(
            """INSERT OR IGNORE INTO events
               (source, line_no, table_name, pk, version, ts, op, after_json, schema_version)
               VALUES (:source, :line_no, :table, :pk, :version, :ts, :op, :after_json, :schema_version)""",
            row,
        )
        inserted += cursor.rowcount
    conn.commit()
    return {"attempted": len(events), "inserted": inserted, "skipped": len(events) - inserted}


def seed_sample(conn: sqlite3.Connection, events_dir: Path | None = None) -> dict:
    """把 sample_data/events/*.jsonl 逐表幂等导入；返回本次导入统计。"""
    events_dir = events_dir or settings.EVENTS_DIR
    total = {"attempted": 0, "inserted": 0, "skipped": 0, "tables": {}}
    for item in settings.TABLES:
        path = events_dir / item["file"]
        events = load_events_file(path, item["name"])
        stats = archive_events(conn, events)
        total["attempted"] += stats["attempted"]
        total["inserted"] += stats["inserted"]
        total["skipped"] += stats["skipped"]
        total["tables"][item["name"]] = stats
    set_meta(conn, "seed_version", settings.SEED_VERSION)
    set_meta(conn, "seeded_at", now_iso())
    total["seed_version"] = settings.SEED_VERSION
    return total


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        table=row["table_name"],
        line_no=int(row["line_no"]),
        pk=row["pk"],
        version=int(row["version"]),
        ts=parse_ts(row["ts"]),
        op=row["op"],
        after=json.loads(row["after_json"]),
        schema_version=int(row["schema_version"]),
        source=row["source"],
    )


def fetch_events(conn: sqlite3.Connection, table: str) -> list[Event]:
    """按文件行号（= 到达顺序）返回某表归档事件。"""
    rows = conn.execute(
        "SELECT * FROM events WHERE table_name=? ORDER BY source, line_no", (table,)
    ).fetchall()
    return [_row_to_event(row) for row in rows]


def event_counts(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT table_name, COUNT(*) AS n FROM events GROUP BY table_name"
    ).fetchall()
    return {row["table_name"]: int(row["n"]) for row in rows}


def total_events(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM events").fetchone()
    return int(row["n"])


# --- runs ------------------------------------------------------------------


def save_run(conn: sqlite3.Connection, run: dict) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO runs
           (run_id, created_at, tables_json, schema_version, summary_json, payload_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            run["run_id"],
            run["created_at"],
            json.dumps(run["tables"], ensure_ascii=False),
            int(run["schema_version"]),
            json.dumps(run["summary"], ensure_ascii=False),
            json.dumps(run["result"], ensure_ascii=False),
        ),
    )
    conn.commit()


def get_run(conn: sqlite3.Connection, run_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if row is None:
        return None
    return {
        "run_id": row["run_id"],
        "created_at": row["created_at"],
        "tables": json.loads(row["tables_json"]),
        "schema_version": int(row["schema_version"]),
        "summary": json.loads(row["summary_json"]),
        "result": json.loads(row["payload_json"]),
    }


def list_runs(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        "SELECT run_id, created_at, tables_json, schema_version, summary_json FROM runs "
        "ORDER BY created_at DESC, run_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    out = []
    for row in rows:
        out.append(
            {
                "run_id": row["run_id"],
                "created_at": row["created_at"],
                "tables": json.loads(row["tables_json"]),
                "schema_version": int(row["schema_version"]),
                "summary": json.loads(row["summary_json"]),
            }
        )
    return out


def run_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
    return int(row["n"])


def event_overview(conn: sqlite3.Connection) -> dict[str, dict]:
    """首页概览：按表统计 op / schema 版本 / 键数 / 时间区间（全部来自归档表）。"""
    out: dict[str, dict] = {}
    for item in settings.TABLES:
        table = item["name"]
        rows = conn.execute(
            "SELECT op, schema_version, pk, ts FROM events WHERE table_name=?", (table,)
        ).fetchall()
        ops: dict[str, int] = {}
        versions: dict[str, int] = {}
        stamps: list[str] = []
        keys: set[str] = set()
        for row in rows:
            ops[row["op"]] = ops.get(row["op"], 0) + 1
            versions[str(row["schema_version"])] = versions.get(str(row["schema_version"]), 0) + 1
            keys.add(row["pk"])
            stamps.append(row["ts"])
        out[table] = {
            "table": table,
            "label": item["label"],
            "desc": item["desc"],
            "file": item["file"],
            "events": len(rows),
            "ops": ops,
            "schema_versions": versions,
            "keys": len(keys),
            "first_ts": min(stamps) if stamps else None,
            "last_ts": max(stamps) if stamps else None,
        }
    return out


# --- 模块 7：来源水位线 ----------------------------------------------------


def upsert_source_watermark(
    conn: sqlite3.Connection, source: str, watermark_ts: str, note: str = ""
) -> None:
    """登记/推进一个来源的安全水位线（回退由 retention 层先校验，这里只落库）。"""
    conn.execute(
        """INSERT INTO source_watermarks(source, watermark_ts, note, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(source) DO UPDATE SET
             watermark_ts=excluded.watermark_ts,
             note=excluded.note,
             updated_at=excluded.updated_at""",
        (source, watermark_ts, note, now_iso()),
    )
    conn.commit()


def fetch_source_watermarks(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        "SELECT source, watermark_ts, note, updated_at FROM source_watermarks "
        "ORDER BY watermark_ts, source"
    ).fetchall()
    return [
        {
            "source": row["source"],
            "watermark": row["watermark_ts"],
            "note": row["note"],
            "updated_at": row["updated_at"],
        }
        for row in rows
    ]


# --- 模块 7：压缩作业 ------------------------------------------------------


def save_compaction_job(conn: sqlite3.Connection, job: dict) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO compaction_jobs
           (job_id, created_at, updated_at, status, tables_json, boundary_json,
            completed_json, plan_json, verification_json, summary_json, error)
           VALUES (:job_id, :created_at, :updated_at, :status, :tables_json, :boundary_json,
                   :completed_json, :plan_json, :verification_json, :summary_json, :error)""",
        {
            "job_id": job["job_id"],
            "created_at": job["created_at"],
            "updated_at": job.get("updated_at", job["created_at"]),
            "status": job["status"],
            "tables_json": json.dumps(job["tables"], ensure_ascii=False),
            "boundary_json": json.dumps(job["boundary"], ensure_ascii=False),
            "completed_json": json.dumps(job.get("completed", []), ensure_ascii=False),
            "plan_json": json.dumps(job.get("plan", {}), ensure_ascii=False),
            "verification_json": json.dumps(job.get("verification", {}), ensure_ascii=False),
            "summary_json": json.dumps(job.get("summary", {}), ensure_ascii=False),
            "error": job.get("error", ""),
        },
    )
    conn.commit()


def _job_row_to_dict(row: sqlite3.Row) -> dict:
    return {
        "job_id": row["job_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "status": row["status"],
        "tables": json.loads(row["tables_json"]),
        "boundary": json.loads(row["boundary_json"]),
        "completed": json.loads(row["completed_json"]),
        "plan": json.loads(row["plan_json"]),
        "verification": json.loads(row["verification_json"]),
        "summary": json.loads(row["summary_json"]),
        "error": row["error"],
    }


def get_compaction_job(conn: sqlite3.Connection, job_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM compaction_jobs WHERE job_id=?", (job_id,)
    ).fetchone()
    return _job_row_to_dict(row) if row else None


def list_compaction_jobs(conn: sqlite3.Connection, limit: int = 20) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM compaction_jobs ORDER BY created_at DESC, job_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [_job_row_to_dict(row) for row in rows]


def find_resumable_job(conn: sqlite3.Connection, tables: list[str]) -> dict | None:
    """找与本表清单一致、仍在 running 的作业（中断后续跑的入口）。"""
    rows = conn.execute(
        "SELECT * FROM compaction_jobs WHERE status='running' ORDER BY created_at DESC"
    ).fetchall()
    wanted = set(tables)
    for row in rows:
        if set(json.loads(row["tables_json"])) == wanted:
            return _job_row_to_dict(row)
    return None


def count_compaction_jobs(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM compaction_jobs").fetchone()
    return int(row["n"])


# --- 模块 7：压缩墓碑归档 --------------------------------------------------


def save_compaction_baselines(conn, job_id: str, baselines: dict[str, dict]) -> None:
    """记录作业切换瞬间各表的归档基线（最大行号 + 事件数）。

    不在此函数内 commit：调用方把它与墓碑归档放在同一事务里，任一失败整体回滚。
    """
    for table, item in baselines.items():
        conn.execute(
            """INSERT OR REPLACE INTO compaction_baselines
               (job_id, table_name, max_line_no, event_count, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (job_id, table, int(item["max_line_no"]), int(item["event_count"]), now_iso()),
        )


def guard_baseline(conn, table: str) -> dict | None:
    """该表最近一次成功压缩的归档基线（行号口径），供重放守卫区分补传事件。

    取 max_line_no 最大的一条：多次压缩后，只有晚于最后一次压缩基线的事件才算补传。
    """
    row = conn.execute(
        "SELECT * FROM compaction_baselines WHERE table_name=? "
        "ORDER BY max_line_no DESC, created_at DESC LIMIT 1",
        (table,),
    ).fetchone()
    if row is None:
        return None
    return {
        "job_id": row["job_id"],
        "table": row["table_name"],
        "max_line_no": int(row["max_line_no"]),
        "event_count": int(row["event_count"]),
    }


def insert_compacted_tombstones(
    conn: sqlite3.Connection,
    job_id: str,
    table: str,
    decisions: list[dict],
    boundary_ts: str,
    tombstone_payloads: dict[str, dict],
) -> int:
    """把一张表判定可压缩的墓碑搬入归档；同键重复压缩由主键兜底忽略。

    `tombstone_payloads` 按 pk 给出当前墓碑完整行（合并结果里的 tombstone dict）。
    返回新归档条数。
    """
    inserted = 0
    for item in decisions:
        if not item["eligible"]:
            continue
        payload = tombstone_payloads.get(item["pk"], {})
        cursor = conn.execute(
            """INSERT OR IGNORE INTO compacted_tombstones
               (job_id, table_name, pk, tombstone_version, tombstone_line_no,
                tombstone_ts, boundary_ts, compacted_at, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                table,
                item["pk"],
                int(item["tombstone_version"]),
                int(item["tombstone_line_no"]),
                item["tombstone_ts"],
                boundary_ts,
                now_iso(),
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        inserted += cursor.rowcount
    return inserted


def fetch_compacted_map(conn: sqlite3.Connection, table: str) -> dict[str, dict]:
    """单表已压缩墓碑，按 pk 索引（重放拦截与合并过滤共用）。"""
    rows = conn.execute(
        "SELECT * FROM compacted_tombstones WHERE table_name=?", (table,)
    ).fetchall()
    return {
        row["pk"]: {
            "job_id": row["job_id"],
            "table": row["table_name"],
            "pk": row["pk"],
            "tombstone_version": int(row["tombstone_version"]),
            "tombstone_line_no": int(row["tombstone_line_no"]),
            "tombstone_ts": row["tombstone_ts"],
            "boundary_ts": row["boundary_ts"],
            "compacted_at": row["compacted_at"],
            "payload": json.loads(row["payload_json"]),
        }
        for row in rows
    }


def fetch_compacted_tombstones(conn: sqlite3.Connection, table: str | None = None) -> list[dict]:
    if table:
        rows = conn.execute(
            "SELECT * FROM compacted_tombstones WHERE table_name=? "
            "ORDER BY compacted_at DESC, pk",
            (table,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM compacted_tombstones ORDER BY compacted_at DESC, pk"
        ).fetchall()
    return [
        {
            "job_id": row["job_id"],
            "table": row["table_name"],
            "pk": row["pk"],
            "tombstone_version": int(row["tombstone_version"]),
            "tombstone_line_no": int(row["tombstone_line_no"]),
            "tombstone_ts": row["tombstone_ts"],
            "boundary_ts": row["boundary_ts"],
            "compacted_at": row["compacted_at"],
            "payload": json.loads(row["payload_json"]),
        }
        for row in rows
    ]


def compacted_count(conn: sqlite3.Connection, table: str | None = None) -> int:
    if table:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM compacted_tombstones WHERE table_name=?", (table,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) AS n FROM compacted_tombstones").fetchone()
    return int(row["n"])


# --- 模块 7：拦截重放 ------------------------------------------------------


def record_blocked_replay(
    conn: sqlite3.Connection,
    event: Event,
    compacted: dict,
    reason: str,
    job_id: str = "",
) -> bool:
    """登记一条被拦截的补传重放；(source, line_no) 唯一保证重跑不重复计数。

    返回 True 表示新登记，False 表示此前已登记（幂等重跑）。
    """
    cursor = conn.execute(
        """INSERT OR IGNORE INTO blocked_replays
           (job_id, source, line_no, table_name, pk, event_version, event_ts,
            op, tombstone_version, reason, blocked_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            job_id,
            event.source,
            event.line_no,
            event.table,
            event.pk,
            event.version,
            event.ts_iso,
            event.op,
            int(compacted["tombstone_version"]),
            reason,
            now_iso(),
        ),
    )
    conn.commit()
    return cursor.rowcount > 0


def fetch_blocked_replays(conn: sqlite3.Connection, table: str | None = None) -> list[dict]:
    if table:
        rows = conn.execute(
            "SELECT * FROM blocked_replays WHERE table_name=? ORDER BY blocked_at DESC, id",
            (table,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM blocked_replays ORDER BY blocked_at DESC, id"
        ).fetchall()
    return [
        {
            "job_id": row["job_id"],
            "source": row["source"],
            "line_no": int(row["line_no"]),
            "table": row["table_name"],
            "pk": row["pk"],
            "event_version": int(row["event_version"]),
            "event_ts": row["event_ts"],
            "op": row["op"],
            "tombstone_version": int(row["tombstone_version"]),
            "reason": row["reason"],
            "blocked_at": row["blocked_at"],
        }
        for row in rows
    ]


def blocked_replay_count(conn: sqlite3.Connection, table: str | None = None) -> int:
    if table:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM blocked_replays WHERE table_name=?", (table,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) AS n FROM blocked_replays").fetchone()
    return int(row["n"])
