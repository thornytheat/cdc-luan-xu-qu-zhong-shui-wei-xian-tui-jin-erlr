"""FastAPI 路由（薄）：页面渲染 + JSON 端点，业务全部在领域模块里。

页面：/ 首页 · /workbench 工作台 · /result/{run_id} 结果 · /review 复核与导出
接口：POST /api/merge · GET /api/runs · GET /api/runs/{id} · GET /api/runs/{id}/snapshot
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from . import archive, compaction_runner, runner, schema_gate, settings, snapshot, store
from .archive import EventError
from .compaction_runner import JobError
from .retention import RetentionError, SourceWatermark, boundary_views, register_source
from .archive import parse_ts

BASE_DIR = settings.BASE_DIR


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = store.connect()
    store.init_db(conn)
    if store.total_events(conn) == 0:
        store.seed_sample(conn)
    conn.close()
    yield


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))


class MergeRequest(BaseModel):
    tables: list[str] = Field(default_factory=list, description="要合并的表名")
    schema_version: int = Field(2, ge=1, le=2, description="要评估的目标 schema 版本")


class SourceWatermarkRequest(BaseModel):
    source: str = Field(..., description="来源名称，如 binlog / mq / backfill")
    watermark: str = Field(..., description="该来源的安全水位线（ISO 时间）")
    note: str = Field("", description="备注")


class CompactionRequest(BaseModel):
    tables: list[str] = Field(default_factory=list, description="要压缩墓碑的表名；空=全部表")
    sources: list[SourceWatermarkRequest] | None = Field(
        None, description="可选：本次随请求登记的来源水位线"
    )


def render(request: Request, template: str, **ctx) -> HTMLResponse:
    context = {
        "app_name": settings.APP_NAME,
        "app_short": settings.APP_SHORT,
        "tables_meta": settings.TABLES,
        "kind_labels": settings.KIND_LABELS,
        "verdict_labels": settings.VERDICT_LABELS,
        "change_labels": settings.CHANGE_LABELS,
        "op_labels": settings.OP_LABELS,
        "nav": ctx.pop("nav", ""),
    }
    context.update(ctx)
    return templates.TemplateResponse(request, template, context)


def with_conn():
    conn = store.connect()
    store.init_db(conn)
    return conn


# --- 页面 ------------------------------------------------------------------


@app.get("/health", include_in_schema=False)
def health():
    conn = with_conn()
    try:
        return {
            "status": "ok",
            "app": settings.APP_SHORT,
            "events": store.total_events(conn),
            "runs": store.run_count(conn),
            "compaction_jobs": store.count_compaction_jobs(conn),
            "compacted_tombstones": store.compacted_count(conn),
            "blocked_replays": store.blocked_replay_count(conn),
            "seed_version": store.get_meta(conn, "seed_version"),
        }
    finally:
        conn.close()


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    conn = with_conn()
    try:
        overview = store.event_overview(conn)
        meta = {
            "seed_version": store.get_meta(conn, "seed_version"),
            "seeded_at": store.get_meta(conn, "seeded_at"),
            "total_events": store.total_events(conn),
            "runs": store.run_count(conn),
        }
        recent = store.list_runs(conn, limit=3)
    finally:
        conn.close()
    stats = {
        "total_events": meta["total_events"],
        "table_count": len(settings.TABLES),
        "runs": meta["runs"],
        "last_run_id": recent[0]["run_id"] if recent else None,
        "last_run_at": recent[0]["created_at"] if recent else None,
        "last_conflicts": recent[0]["summary"]["conflicts"] if recent else 0,
    }
    return render(
        request,
        "index.html",
        page="index",
        nav="index",
        overview=overview,
        meta=meta,
        recent=recent,
        stats=stats,
        embedded=settings.EMBEDDED,
        steps=settings.DEMO_STEPS,
        schema_v1=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        schema_v2=schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
        changes=schema_gate.structural_changes(
            schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
            schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
            settings.TABLE_NAMES,
        ),
    )


@app.get("/workbench", response_class=HTMLResponse)
def workbench(request: Request):
    conn = with_conn()
    try:
        overview = store.event_overview(conn)
        recent = store.list_runs(conn, limit=5)
    finally:
        conn.close()
    return render(
        request,
        "workbench.html",
        page="workbench",
        nav="workbench",
        overview=overview,
        recent=recent,
        schema_v1=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        schema_v2=schema_gate.load_schema(settings.SCHEMAS_DIR / "v2.json"),
    )


@app.get("/result/{run_id}", response_class=HTMLResponse)
def result_page(request: Request, run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
        if run is None:
            return render(
                request,
                "not_found.html",
                page="not_found",
                nav="",
                run_id=run_id,
                error=f"没有找到运行 {run_id}，可能已被清理；请到工作台重新执行一次合并。",
            )
        snap = snapshot.build_snapshot(run, conn=conn)
        preview = snapshot.preview(snap)
    finally:
        conn.close()
    result = run["result"]
    ignored = {
        table: set(payload.get("ignored_line_nos", []))
        for table, payload in result["tables"].items()
    }
    change_by_line = {(entry["table"], entry["line_no"]): entry for entry in result["changelog"]}
    late_rows = []
    for item in result["late_events"]:
        row = dict(item)
        entry = change_by_line.get((item["table"], item["line_no"]), {})
        row["after_tombstone"] = item["line_no"] in ignored.get(item["table"], set())
        row["kind"] = entry.get("kind", "")
        row["kind_label"] = entry.get("kind_label", "已处理")
        row["reason"] = entry.get("reason", "")
        late_rows.append(row)
    return render(
        request,
        "result.html",
        page="result",
        nav="",
        run=run,
        summary=run["summary"],
        result=result,
        snapshot=snap,
        preview=preview,
        late_rows=late_rows,
        snapshot_bytes=snapshot.snapshot_size(snap),
        base_schema=schema_gate.load_schema(settings.SCHEMAS_DIR / "v1.json"),
        target_schema=schema_gate.load_schema(
            settings.SCHEMAS_DIR / f"v{run['schema_version']}.json"
        ),
    )


@app.get("/review", response_class=HTMLResponse)
def review(request: Request, run_id: str | None = None, kind: str = Query(default="")):
    conn = with_conn()
    try:
        runs = store.list_runs(conn, limit=50)
        selected = store.get_run(conn, run_id) if run_id else (
            store.get_run(conn, runs[0]["run_id"]) if runs else None
        )
    finally:
        conn.close()
    entries: list[dict] = []
    kinds: list[str] = []
    if selected is not None:
        entries = selected["result"]["changelog"]
        kinds = sorted({entry["kind"] for entry in entries})
        if kind:
            entries = [entry for entry in entries if entry["kind"] == kind]
        entries = entries[:200]
    return render(
        request,
        "review.html",
        page="review",
        nav="review",
        runs=runs,
        selected=selected,
        entries=entries,
        kinds=kinds,
        active_kind=kind,
        run_count=len(runs),
    )


# --- API -------------------------------------------------------------------


@app.post("/api/merge")
def api_merge(payload: MergeRequest):
    conn = with_conn()
    try:
        run = runner.execute(conn, payload.tables, payload.schema_version)
    except runner.RunError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except schema_gate.SchemaError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EventError as exc:
        raise HTTPException(status_code=422, detail=f"样例事件解析失败：{exc}") from exc
    finally:
        conn.close()
    return JSONResponse(
        {
            "run_id": run["run_id"],
            "summary": run["summary"],
            "tables": run["tables"],
            "schema_version": run["schema_version"],
            "result_url": f"/result/{run['run_id']}",
            "snapshot_url": f"/api/runs/{run['run_id']}/snapshot",
        }
    )


@app.get("/api/runs")
def api_runs(limit: int = Query(default=20, ge=1, le=100)):
    conn = with_conn()
    try:
        runs = store.list_runs(conn, limit=limit)
    finally:
        conn.close()
    return {"runs": runs, "count": len(runs)}


@app.get("/api/runs/{run_id}")
def api_run(run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
    finally:
        conn.close()
    if run is None:
        raise HTTPException(status_code=404, detail=f"运行不存在：{run_id}")
    result = run["result"]
    return {
        "run_id": run["run_id"],
        "created_at": run["created_at"],
        "tables": run["tables"],
        "schema_version": run["schema_version"],
        "summary": run["summary"],
        "watermark": {
            "watermark": result["watermark"]["watermark"],
            "open_keys": result["watermark"]["open_keys"],
            "closed_keys": result["watermark"]["closed_keys"],
            "per_table": {
                table: {
                    "watermark": payload["watermark"]["watermark"],
                    "open_keys": payload["watermark"]["open_keys"],
                    "closed_keys": payload["watermark"]["closed_keys"],
                    "laggard_keys": payload["watermark"]["laggard_keys"],
                }
                for table, payload in result["tables"].items()
            },
        },
        "tables": {
            table: {
                "row_count": payload["row_count"],
                "tombstone_count": payload["tombstone_count"],
                "key_count": payload["key_count"],
                "counters": payload["counters"],
            }
            for table, payload in result["tables"].items()
        },
        "gate": result["gate"],
        "conflicts": [
            {
                "table": item["table"],
                "pk": item["pk"],
                "version": item["version"],
                "diff": item["diff"],
                "first_line": item["first"]["line_no"],
                "second_line": item["second"]["line_no"],
            }
            for item in result["conflicts"]
        ],
        "late_events": result["late_events"],
        "snapshot_url": f"/api/runs/{run['run_id']}/snapshot",
    }


@app.get("/api/runs/{run_id}/snapshot")
def api_snapshot(run_id: str):
    conn = with_conn()
    try:
        run = store.get_run(conn, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"运行不存在：{run_id}")
        payload = snapshot.dumps(snapshot.build_snapshot(run, conn=conn))
    finally:
        conn.close()
    filename = f"cdcwater-snapshot-{run_id}.json"
    return Response(
        content=payload,
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/seed")
def api_seed():
    """幂等重放样例：已归档的事件不会重复插入。"""
    conn = with_conn()
    try:
        stats = store.seed_sample(conn)
        stats["total_events"] = store.total_events(conn)
        stats["counts"] = store.event_counts(conn)
    except EventError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    finally:
        conn.close()
    return stats


@app.get("/api/samples")
def api_samples():
    conn = with_conn()
    try:
        return {"overview": store.event_overview(conn), "total": store.total_events(conn)}
    finally:
        conn.close()


@app.get("/api/schemas/{version}")
def api_schema(version: int):
    if version not in (1, 2):
        raise HTTPException(status_code=400, detail="schema 版本只支持 1 或 2")
    return schema_gate.load_schema(settings.SCHEMAS_DIR / f"v{version}.json")


# --- 模块 7：墓碑压缩 ------------------------------------------------------


def _load_registered_sources(conn) -> dict[str, SourceWatermark]:
    sources: dict[str, SourceWatermark] = {}
    for row in store.fetch_source_watermarks(conn):
        sources[row["source"]] = SourceWatermark(
            source=row["source"], watermark=parse_ts(row["watermark"]), note=row.get("note", "")
        )
    return sources


def _register_request_sources(conn, items: list[SourceWatermarkRequest] | None) -> None:
    if not items:
        return
    sources = _load_registered_sources(conn)
    for item in items:
        entry = register_source(sources, item.source, item.watermark, item.note)
        store.upsert_source_watermark(conn, entry.source, entry.watermark_iso, entry.note)


@app.get("/compaction", response_class=HTMLResponse)
def compaction_page(request: Request):
    """墓碑压缩工作台：来源边界、作业历史、归档与拦截记录。"""
    conn = with_conn()
    try:
        sources = _load_registered_sources(conn)
        jobs = store.list_compaction_jobs(conn, limit=20)
        archived = store.fetch_compacted_tombstones(conn)
        blocked = store.fetch_blocked_replays(conn)
        overview = store.event_overview(conn)
    finally:
        conn.close()
    return render(
        request,
        "compaction.html",
        page="compaction",
        nav="compaction",
        boundary=boundary_views(sources),
        sources_raw=sources,
        jobs=jobs,
        archived=archived,
        blocked=blocked,
        overview=overview,
        job_status_labels=settings.JOB_STATUS_LABELS,
        vote_labels=settings.VOTE_LABELS,
    )


@app.get("/compaction/{job_id}", response_class=HTMLResponse)
def compaction_result_page(request: Request, job_id: str):
    conn = with_conn()
    try:
        try:
            detail = compaction_runner.job_detail(conn, job_id)
        except JobError:
            return render(
                request,
                "not_found.html",
                page="not_found",
                nav="compaction",
                run_id=job_id,
                error=f"没有找到压缩作业 {job_id}；请到墓碑压缩页重新发起。",
            )
    finally:
        conn.close()
    return render(
        request,
        "compaction_result.html",
        page="compaction",
        nav="compaction",
        job=detail,
        vote_labels=settings.VOTE_LABELS,
        job_status_labels=settings.JOB_STATUS_LABELS,
    )


@app.get("/api/retention/sources")
def api_list_sources():
    conn = with_conn()
    try:
        sources = _load_registered_sources(conn)
        return boundary_views(sources)
    finally:
        conn.close()


@app.post("/api/retention/sources")
def api_register_source(payload: SourceWatermarkRequest):
    conn = with_conn()
    try:
        sources = _load_registered_sources(conn)
        try:
            entry = register_source(sources, payload.source, payload.watermark, payload.note)
        except RetentionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        store.upsert_source_watermark(conn, entry.source, entry.watermark_iso, entry.note)
        return boundary_views(_load_registered_sources(conn))
    finally:
        conn.close()


@app.post("/api/compaction/jobs")
def api_create_compaction(payload: CompactionRequest):
    conn = with_conn()
    try:
        try:
            _register_request_sources(conn, payload.sources)
            tables = payload.tables or settings.TABLE_NAMES
            job = compaction_runner.start_job(conn, tables)
        except (JobError, RetentionError, runner.RunError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()
    return JSONResponse(
        {
            "job_id": job["job_id"],
            "status": job["status"],
            "tables": job["tables"],
            "completed": job["completed"],
            "boundary": job["boundary"],
            "summary": job["summary"],
            "verification": job["verification"],
            "result_url": f"/compaction/{job['job_id']}",
        }
    )

@app.get("/api/compaction/jobs")
def api_list_compaction_jobs(limit: int = Query(default=20, ge=1, le=100)):
    conn = with_conn()
    try:
        jobs = store.list_compaction_jobs(conn, limit=limit)
    finally:
        conn.close()
    return {"jobs": jobs, "count": len(jobs)}


@app.get("/api/compaction/jobs/{job_id}")
def api_get_compaction_job(job_id: str):
    conn = with_conn()
    try:
        try:
            detail = compaction_runner.job_detail(conn, job_id)
        except JobError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        conn.close()
    return detail


@app.post("/api/compaction/jobs/{job_id}/resume")
def api_resume_compaction_job(job_id: str):
    """中断续跑：从已完成表清单之后继续；completed 作业幂等返回历史结果。"""
    conn = with_conn()
    try:
        if store.get_compaction_job(conn, job_id) is None:
            raise HTTPException(status_code=404, detail=f"压缩作业不存在：{job_id}")
        try:
            job = compaction_runner.run_job(conn, job_id)
        except (JobError, RetentionError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        conn.close()
    return {
        "job_id": job["job_id"],
        "status": job["status"],
        "tables": job["tables"],
        "completed": job["completed"],
        "boundary": job["boundary"],
        "summary": job["summary"],
        "verification": job["verification"],
        "result_url": f"/compaction/{job['job_id']}",
    }


@app.get("/api/compaction/archived")
def api_archived(table: str | None = None):
    conn = with_conn()
    try:
        rows = store.fetch_compacted_tombstones(conn, table)
        return {"archived": rows, "count": len(rows)}
    finally:
        conn.close()


@app.get("/api/compaction/blocked-replays")
def api_blocked_replays(table: str | None = None):
    conn = with_conn()
    try:
        rows = store.fetch_blocked_replays(conn, table)
        return {"blocked_replays": rows, "count": len(rows)}
    finally:
        conn.close()
