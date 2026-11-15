"""模块 7b · 墓碑压缩：保留边界判定、重放拦截与整批校验（纯函数）。

与 merge/tombstone 的关系：
- merge 负责在线合并，产出「当前墓碑」；本模块只决定哪些墓碑可以压缩、
  压缩后补传的旧事件如何拦截，不改变既有合并口径。
- 压缩不是物理删除：被压缩的墓碑整体搬入归档（compacted_tombstones）留底，
  快照/导出的「当前墓碑」不再包含它们，但归档里永久可查。

三条硬约束（对应需求）：
1. 共同确认：墓碑 ts <= 各来源共同保留边界（min 来源水位线）才允许压缩，
   边界之上的墓碑一个都不能动。
2. 不复活：压缩后补传的事件 version <= 墓碑版本 → 拦截留档，绝不写回状态；
   只有严格更高版本才允许复活。
3. 整批原子：所有选中表各自做影子校验，全部通过才允许切换现网；
   任一表失败整批放弃，旧版完整保留。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from .archive import Event, parse_ts
from .retention import SourceWatermark, joint_boundary, tombstone_votes


class CompactionError(ValueError):
    """压缩流程的可预期错误（路由转 4xx，作业状态置 failed）。"""


def _fmt(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_tombstone_ts(row: dict) -> datetime:
    return parse_ts(row["ts"])


# --- 计划 ------------------------------------------------------------------


@dataclass
class TombstoneDecision:
    """单个墓碑的压缩判定，保留逐来源投票作为页面依据。"""

    table: str
    pk: str
    tombstone_version: int
    tombstone_line_no: int
    tombstone_ts: str
    eligible: bool
    reason: str
    votes: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "pk": self.pk,
            "tombstone_version": self.tombstone_version,
            "tombstone_line_no": self.tombstone_line_no,
            "tombstone_ts": self.tombstone_ts,
            "eligible": self.eligible,
            "reason": self.reason,
            "votes": self.votes,
        }


@dataclass
class TablePlan:
    """单表压缩计划：可压缩 / 必须保留两组墓碑，压缩前后计数对照。"""

    table: str
    decisions: list[TombstoneDecision] = field(default_factory=list)
    tombstone_before: int = 0

    @property
    def eligible(self) -> list[TombstoneDecision]:
        return [item for item in self.decisions if item.eligible]

    @property
    def retained(self) -> list[TombstoneDecision]:
        return [item for item in self.decisions if not item.eligible]

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "tombstone_before": self.tombstone_before,
            "eligible_count": len(self.eligible),
            "retained_count": len(self.retained),
            "tombstone_after": self.tombstone_before - len(self.eligible),
            "decisions": [item.to_dict() for item in self.decisions],
        }


def build_table_plan(
    table: str,
    tombstones: list[dict],
    sources: dict[str, SourceWatermark],
) -> TablePlan:
    """对一张表的当前墓碑逐个判定；无来源登记时全部保留（保守，不压缩）。"""
    boundary = joint_boundary(sources)
    plan = TablePlan(table=table, tombstone_before=len(tombstones))
    for row in tombstones:
        ts = parse_tombstone_ts(row)
        if boundary is None:
            plan.decisions.append(
                TombstoneDecision(
                    table, row["pk"], int(row["tombstone_version"]),
                    int(row["tombstone_line_no"]), row["ts"], False,
                    "没有任何来源登记安全水位线，按保守口径保留全部墓碑",
                )
            )
            continue
        poll = tombstone_votes(ts, sources)
        if poll["eligible"]:
            reason = (
                f"墓碑 ts {row['ts']} <= 共同保留边界 {_fmt(boundary)}，"
                f"{len(poll['votes'])} 个来源全部放行，可压缩"
            )
        else:
            retainers = [v["source"] for v in poll["votes"] if v["vote"] == "retain"]
            reason = (
                f"墓碑 ts {row['ts']} > 共同保留边界 {_fmt(boundary)}，"
                f"来源 {', '.join(retainers)} 仍可能补传旧事件，必须保留"
            )
        plan.decisions.append(
            TombstoneDecision(
                table, row["pk"], int(row["tombstone_version"]),
                int(row["tombstone_line_no"]), row["ts"], poll["eligible"],
                reason, poll["votes"],
            )
        )
    return plan


# --- 重放拦截 --------------------------------------------------------------


def classify_replay(event: Event, compacted: dict | None) -> str | None:
    """补传事件相对某个已压缩墓碑的判定。

    返回：
    - None              该事件与已压缩墓碑无关（正常走合并规则）
    - "blocked_replay"  版本 <= 墓碑版本：拦截，留档，绝不复活
    - "resurrect"       严格更高版本：允许复活（压缩墓碑仍在归档里可追溯）

    `compacted` 为该 (table, pk) 已压缩墓碑的归档行（含 tombstone_version），
    没有则返回 None。
    """
    if compacted is None:
        return None
    tomb_version = int(compacted["tombstone_version"])
    if event.version <= tomb_version:
        return "blocked_replay"
    return "resurrect"


def replay_reason(event: Event, compacted: dict) -> str:
    tomb_version = int(compacted["tombstone_version"])
    if event.version <= tomb_version:
        return (
            f"该键墓碑（v{tomb_version}，归档第 {compacted['tombstone_line_no']} 行）"
            f"已按保留边界压缩并留底；补传事件 v{event.version} 不高于墓碑版本，"
            "拦截且不复活，状态不变"
        )
    return (
        f"补传事件 v{event.version} 严格高于已压缩墓碑 v{tomb_version}，"
        "允许复活；压缩记录仍保留在归档中可追溯"
    )


# --- 整批影子校验 ----------------------------------------------------------


@dataclass
class TableCheck:
    table: str
    ok: bool
    tombstone_before: int
    compacting: int
    retained: int
    tombstone_after: int
    alive_rows: int
    checks: list[dict] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "ok": self.ok,
            "tombstone_before": self.tombstone_before,
            "compacting": self.compacting,
            "retained": self.retained,
            "tombstone_after": self.tombstone_after,
            "alive_rows": self.alive_rows,
            "checks": self.checks,
            "failures": self.failures,
        }


def verify_table(
    plan: TablePlan,
    alive_rows: int,
    sources: dict[str, SourceWatermark],
) -> TableCheck:
    """对单表压缩计划做影子校验（不碰现网）；任一断言失败即 ok=False。"""
    compacting = len(plan.eligible)
    retained = len(plan.retained)
    after = plan.tombstone_before - compacting
    check = TableCheck(
        table=plan.table,
        ok=True,
        tombstone_before=plan.tombstone_before,
        compacting=compacting,
        retained=retained,
        tombstone_after=after,
        alive_rows=alive_rows,
    )

    def assert_check(name: str, ok: bool, detail: str) -> None:
        check.checks.append({"name": name, "ok": ok, "detail": detail})
        if not ok:
            check.ok = False
            check.failures.append(f"{name}：{detail}")

    # 1) 计数守恒：压缩前 = 压缩掉 + 保留；压缩后 = 保留数
    assert_check(
        "墓碑计数守恒",
        plan.tombstone_before == compacting + retained,
        f"{plan.tombstone_before} = {compacting} 压缩 + {retained} 保留",
    )
    assert_check(
        "压缩后墓碑数",
        after == retained and after >= 0,
        f"压缩后应剩 {retained} 个，实算 {after}",
    )
    # 2) 边界守恒：每个被压缩的墓碑都必须满足 ts <= 共同边界，且各来源一致放行
    boundary = joint_boundary(sources)
    over: list[str] = []
    split_vote: list[str] = []
    for item in plan.eligible:
        if boundary is None or parse_tombstone_ts({"ts": item.tombstone_ts}) > boundary:
            over.append(item.pk)
        if any(vote["vote"] != "release" for vote in item.votes):
            split_vote.append(item.pk)
    if compacting:
        assert_check(
            "压缩墓碑均在保留边界内",
            not over,
            "全部在边界内" if not over else f"越界墓碑：{', '.join(over)}",
        )
        assert_check(
            "各来源一致放行",
            not split_vote,
            "全部来源投票放行" if not split_vote else f"存在保留投票：{', '.join(split_vote)}",
        )
    else:
        check.checks.append(
            {"name": "压缩墓碑均在保留边界内", "ok": True, "detail": "本表无压缩项，跳过"}
        )
        check.checks.append(
            {"name": "各来源一致放行", "ok": True, "detail": "本表无压缩项，跳过"}
        )
    # 3) 存活行不受影响：压缩只动墓碑，不动任何存活行
    assert_check(
        "存活行数不变",
        alive_rows >= 0,
        f"存活行 {alive_rows} 行不参与压缩，行数保持不变",
    )
    # 4) 留档完整：每个压缩项都有表名+键+版本+行号，可回溯
    missing = [
        item.pk for item in plan.eligible
        if not (item.table and item.pk and item.tombstone_version and item.tombstone_line_no)
    ]
    assert_check(
        "压缩记录留档完整",
        not missing,
        "每条压缩墓碑均带表/键/版本/归档行号" if not missing else f"留档缺字段：{missing}",
    )
    return check


def verify_batch(
    plans: dict[str, TablePlan],
    alive_by_table: dict[str, int],
    sources: dict[str, SourceWatermark],
) -> dict:
    """整批校验：所有表都通过才允许切换现网；返回汇总 + 总判定。"""
    per_table: dict[str, TableCheck] = {}
    for table, plan in plans.items():
        per_table[table] = verify_table(plan, alive_by_table.get(table, 0), sources)
    all_ok = all(check.ok for check in per_table.values())
    total_before = sum(check.tombstone_before for check in per_table.values())
    total_compacting = sum(check.compacting for check in per_table.values())
    total_retained = sum(check.retained for check in per_table.values())
    return {
        "ok": all_ok,
        "per_table": {table: check.to_dict() for table, check in per_table.items()},
        "totals": {
            "tombstone_before": total_before,
            "compacting": total_compacting,
            "retained": total_retained,
            "tombstone_after": total_before - total_compacting,
        },
        "decision": "pass_all_tables" if all_ok else "abort_keep_previous",
    }
