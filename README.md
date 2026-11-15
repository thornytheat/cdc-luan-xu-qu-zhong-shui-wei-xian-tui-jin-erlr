# cdcwater · CDC 乱序去重与水位线推进

面向数据集成组的 CDC 合并工具：把订单 / 客户 / 库存三库的变更事件（乱序、重复、迟到）
合并成可信的最终行，给出可复现的水位线，并在放行 schema 变更前逐条评估影响；
模块 7 进一步按各来源共同确认的保留边界压缩已撤销订单的墓碑，控制快照/导出存储膨胀。

- 形态：单服务 FastAPI + Jinja2 服务端模板 + 原生 JS，SQLite 命名卷持久化。
- 端口：宿主机 `18213`（容器内 8000）。
- 样例：3 张表 × 60 条事件，故意埋 8 条迟到、3 条同版本冲突、2 条墓碑后迟到、1 条同内容重复。

## 能力

1. **事件归档**：JSONL 逐行解析、字段校验，事件保留「文件 + 行号」作为所有结论的定位依据；导入幂等（唯一键 source + line_no）。
2. **同键合并**：同 pk 保留最大 `version`；同版本同内容静默去重；同版本不同内容判 `conflict` 且不覆盖先到内容。
3. **墓碑处理**：`op=d` 且版本更高 → 键判删除，墓碑保留；墓碑后低版本 c/u 忽略并计入 `ignored_after_tombstone`；只有严格更高版本才复活。
4. **乱序与水位线**：按到达顺序判定迟到（`ts < 当前已处理最大 ts`），迟到仍按版本合并；`watermark = min(每个键已处理事件的最大 ts)`，输出全局/分表水位线、未闭合键数与推进轨迹。
5. **Schema 门禁**：对比 v1 → v2，逐条给「可放行 / 放行+回填 / 阻断」，附列位置与理由。
6. **快照导出**：每表最终行 + 当前墓碑 + 冲突明细 + 迟到位置 + 门禁结论 + 全部变更日志 + 压缩归档/拦截段，JSON 可下载。
7. **墓碑压缩（模块 7）**：见下节。

## 模块 7 · 墓碑压缩（保留边界 + 整批原子 + 重放拦截）

已撤销订单的墓碑一直留在快照和导出里会让存储持续膨胀；但直接清理后，补传的低版本
记录又可能把删除态写回。模块 7 用四条硬约束解决这个矛盾：

1. **各来源共同确认的保留边界**：每个上游来源（binlog / mq / 补传通道）各自登记安全
   水位线，共同保留边界 = `min(所有来源水位线)`。只有 `墓碑 ts <= 共同边界` 且**全部
   来源投票放行**的墓碑才允许压缩；边界之上的墓碑一个都不动。没有任何来源登记时拒绝压缩。
2. **整批校验后才替换现网**：逐表先在内存做影子计划与影子校验（计数守恒、边界守恒、
   各来源一致放行、留档完整、存活行不变），**所有表通过后才在单事务里原子切换**；
   任一表失败整批放弃，旧版完整保留（`abort_keep_previous`）。
3. **中断续跑 + 成功前保留旧版**：作业状态机 `running → completed/failed`，按已完成表
   清单从下一张表续跑；切换前失败不写任何归档。同批重跑幂等：归档主键 `(table,pk)` 与
   拦截唯一键 `(source,line_no)` 去重，不重复增加清理次数，也不会丢掉仍须保留的墓碑。
4. **边界内重放留档且不复活**：压缩不是物理删除，墓碑整体搬入 `compacted_tombstones`
   留底。压缩基线之后补传的事件，`version <= 墓碑版本` 一律拦截写入 `blocked_replays`
   （不进状态机、绝不复活）；只有严格更高版本才放行复活，压缩记录仍可追溯。
   压缩前已归档的历史事件仍走正常合并，不会被误计为拦截。

页面：侧边栏「墓碑压缩」（`/compaction`）登记来源、发起/续跑作业、查看归档与拦截；
作业结果页（`/compaction/{job_id}`）对照**清理前后**逐表计数、**拦截记录**与
**各来源保留边界（逐墓碑投票）**。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET/POST | `/api/retention/sources` | 查看共同边界 / 登记推进来源水位线（回退、非法时间 400） |
| POST | `/api/compaction/jobs` | `{"tables":[...]}` 发起或续跑压缩（空表=全部表） |
| GET | `/api/compaction/jobs[/{id}]` | 作业历史 / 单次作业详情（含归档与拦截） |
| POST | `/api/compaction/jobs/{id}/resume` | 中断后从已完成表续跑（completed 幂等返回） |
| GET | `/api/compaction/archived` | 已压缩墓碑归档（可按 `?table=` 过滤） |
| GET | `/api/compaction/blocked-replays` | 拦截的补传重放记录（可按 `?table=` 过滤） |

## 核心规则口径

| 规则 | 口径 |
|---|---|
| 最大版本 | 同 pk 只保留 `version` 最大的事件；冲突时保留**先到**内容 |
| 同版本 | `after` 按字段排序后逐字比较：一致 → 静默去重（不计冲突）；不一致 → 冲突（列出两条事件与差异字段） |
| 墓碑 | 删除事件写入后不可丢；`version < 墓碑版本` 的事件一律忽略，且不改状态 |
| 迟到 | 文件行序处理，`event.ts < 当前已处理最大 ts`（相等不算迟到），记录表名 + 行号 + 当时最大 ts |
| 水位线 | `min(每个键已处理事件的最大 ts)`；`键最大 ts <= 水位线` 为已闭合键，其余为未闭合键 |
| 门禁 | 新增 nullable → `compatible`；新增带默认值 → `compatible_with_backfill`；删列 / 数值收窄 / 类型族不兼容 / 新增非空无默认 / 可空收紧为非空无默认 → `blocked`；总判定取最严 |

## 启动

```bash
cd batch-20261114b/projects/gb-624
cp .env.example .env            # 可选，默认端口 18213
docker compose up -d --build
docker compose ps               # 等 healthy
curl -s http://127.0.0.1:18213/health
```

打开 <http://127.0.0.1:18213/>。SQLite 落在命名卷 `cdcwater-data:/app/data`；
重建数据：`docker compose down -v` 后重新 `up`（启动时幂等 seed，重复启动不会重复导入事件）。

## 演示步骤

1. 首页看样例概览与埋点说明（8 迟到 / 3 冲突 / 2 墓碑后迟到 / 1 重复）。
2. 工作台勾选 3 张表、目标 schema 选 v2，点「执行合并」，看进度与即时摘要。
3. 结果页：每表合并行数、冲突逐条（两条事件行号 + 差异字段）、迟到位置、水位线、门禁逐条结论、快照预览。
4. 结果页或复核页下载快照 JSON；复核页可按结论筛选 180 条变更日志。

## API / 页面

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查（含事件数、运行数、压缩作业/归档/拦截计数） |
| GET | `/` `/workbench` `/result/{run_id}` `/review` `/compaction[/{job_id}]` | 页面 |
| POST | `/api/merge` | `{"tables":["orders"],"schema_version":2}` → run_id + 摘要 |
| GET | `/api/runs` `/api/runs/{run_id}` | 历史运行 / 单次运行详请 |
| GET | `/api/runs/{run_id}/snapshot` | 下载快照 JSON（attachment，含 compaction 段） |
| POST | `/api/seed` | 幂等重放样例（已归档事件不重复插入） |
| GET | `/api/samples` `/api/schemas/{1\|2}` | 样例概览 / schema 文档 |
| GET/POST | `/api/retention/sources` | 共同保留边界 / 登记来源水位线 |
| POST | `/api/compaction/jobs` `/api/compaction/jobs/{id}/resume` | 发起（或续跑）墓碑压缩 / 显式续跑 |
| GET | `/api/compaction/jobs` `/api/compaction/jobs/{id}` | 作业历史 / 作业详情 |
| GET | `/api/compaction/archived` `/api/compaction/blocked-replays` | 压缩归档 / 拦截重放 |

curl 验收：

```bash
RUN=$(curl -s -X POST http://127.0.0.1:18213/api/merge -H 'Content-Type: application/json' \
  -d '{"tables":["orders","customers","inventory"],"schema_version":2}' | python -c 'import json,sys;print(json.load(sys.stdin)["run_id"])')
curl -s http://127.0.0.1:18213/api/runs/$RUN | python -m json.tool
curl -s -o /tmp/cdcwater-snapshot.json http://127.0.0.1:18213/api/runs/$RUN/snapshot
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://127.0.0.1:18213/api/merge \
  -H 'Content-Type: application/json' -d '{"tables":["ghost"],"schema_version":2}'   # 400
```

## 测试

```bash
python -m pytest -q                               # 本地
docker compose run --rm app python -m pytest -q    # 容器内
```

- `tests/test_merge.py`：去重、同版本冲突/同内容、墓碑、墓碑后忽略、复活、迟到仍按版本合并、样例埋点对齐。
- `tests/test_watermark.py`：迟到判定与位置、水位线公式、未闭合键、跨表合并、推进轨迹单调性。
- `tests/test_schema_gate.py`：三类结论矩阵 + 样例 v1→v2 全量（3 可放行 / 1 回填 / 5 阻断）。
- `tests/test_api.py`：成功、400/404/422、端到端（样例 → 合并 → 结论 → 快照）。
- `tests/test_retention.py`：共同边界取 min、无来源不压缩、水位线回退/非法时间拒绝、逐来源投票。
- `tests/test_compaction.py`：压缩计划边界判定、重放拦截/复活分类、整批校验通过与失败中止。
- `tests/test_compaction_api.py`：端到端压缩、同批重跑幂等、重放拦截留档不复活、断点续跑、失败保留旧版、页面与 404。

## 目录

```text
app/
├── main.py              # FastAPI 路由（薄，含模块 7 路由）
├── settings.py          # 路径、表清单、文案映射
├── archive.py           # 模块1 事件归档（解析/校验/幂等导入）
├── merge.py             # 模块2 同键合并
├── tombstone.py         # 模块3 墓碑/版本判定表（纯函数）
├── watermark.py         # 模块4 迟到与水位线
├── schema_gate.py       # 模块5 schema 门禁
├── snapshot.py          # 模块6 快照导出与页面预览（含 compaction 段）
├── retention.py         # 模块7a 来源水位线/共同保留边界/逐来源投票（纯函数）
├── compaction.py        # 模块7b 压缩计划/重放拦截/整批影子校验（纯函数）
├── compaction_runner.py # 模块7c 作业状态机、断点续跑、整批原子切换
├── runner.py            # 编排 + 落库 + 压缩守卫合并（merge_with_guards）
├── store.py             # SQLite（events/runs/meta + 模块7 四张表）
├── templates/           # base/index/workbench/result/review/not_found + compaction*
└── static/              # app.js / compaction.js / 样式（原生，无框架）
sample_data/
├── generate_samples.py   # 确定性生成器（末尾自检 8 条迟到）
├── events/{orders,customers,inventory}.jsonl
└── schemas/{v1,v2}.json
tests/                      # pytest
```

## 已知限制

- 事件只支持 JSONL 落盘后的批量合并，不做实时流接入；重复跑同一批事件会得到同一结果（幂等）。
- 冲突只判定「同版本不同内容」，不做字段级自动合并；裁决后需人工重放修正事件。
- 水位线按到达顺序的前缀计算，未做事件时间的乱序窗口缓冲（迟到只标注不阻塞）。
- 类型门禁覆盖 string/int/long/decimal/timestamp/boolean 的收窄与族不兼容，不做列重命名识别。
- 队列/上游 connector、鉴权、多租户不在本演示范围。
