# OTLP 尾采样收集器（值班排障用）

接收 OTLP/HTTP JSON，把每条 trace 的 span 先收进 5 秒窗口，窗口结束时做尾采样：
**出错的链、或根 span 耗时超过 500 ms 的链整组留存，其余丢弃**。被丢弃链的原始
span 不在判定事务里删除，而是转入后台按批清理（每批一个事务、最多 200 条），
积压过高时自动暂拒新链。附带网页，可查看采样状态、待清理积压与留存的 span，
并可模拟"入口先报、下游稍后分批上报"以及"批量上报制造清理积压"。

## 运行

```bash
docker compose up --build
# 打开 http://localhost:8000
```

状态保存在命名卷 `collector-data` 中的 SQLite 文件里；`docker compose restart`
后沿用原截止时间与待清理计数，不会重新计时、不会丢积压。

Compose 演示配置把并发上限调到 400、清理间隔放慢到 1 秒：在网页上点
「批量上报制造积压」（默认 400 条 clean 链），等 5 秒窗口到期，约 1200 条
span 同时转入待清理（≥ 1000 阈值），即可看到顶栏「待清理」变红、新链被拒
（原因 "cleanup backlog full"），随后清理循环每批删 200 条，积压降到阈值
以下后接纳自动恢复。

本地（无 Docker）运行：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

## 规则（实现语义）

| 规则 | 实现 |
| --- | --- |
| 每条链收集 5 秒 | 从**首次接纳 span 的受理时刻**起固定 5 秒，截止时间不因后续 span 或重启改变 |
| 整组留存的判定 | 窗口内任一已接纳 span 状态为 ERROR，**或**根 span（无 parentSpanId）耗时 > 500 ms |
| 阈值边界 | 根 span 恰好 500 ms 不算慢，丢弃；报错判定不看窗口外或重复上报的 span |
| 并发上限 8 条链 | 只约束"收集中"的链；满额时新 trace 的 span 被拒收，已有链照常补报 |
| 判定与释放名额同时生效 | 判定是一次事务内的 UPDATE；同一事务内先结算到期链再受理新 span，所以名额立即复用 |
| 丢弃链原文后台清理 | 判定事务只把该链 span 数记入待清理台账，**不删行**；后台循环每批一个事务最多删 `CLEANUP_BATCH_SIZE`（默认 200）条，批次之间让出写锁，上报不被长清理阻塞 |
| 判丢弃即零留存 | 链一旦 dropped：名额立即释放，列表/详情立即显示 0 留存，详情不再返回原始 span（即使行还没物理删除） |
| 清理积压背压 | 待清理 span ≥ `MAX_PENDING_DELETE_SPANS`（默认 1000）时暂拒**新链**（partialSuccess 报 "cleanup backlog full"）；收集中的链照常补报；积压降到阈值以下后按原容量规则恢复接纳 |
| 接纳判断 O(1) | 待清理总数维护在单行台账 `cleanup_state` 中（与判定/清理同事务提交），接纳只读该单行，开销不随积压总量增长 |
| 崩溃/重启恢复 | 台账更新与判定、清理在同一事务；启动时再用库中实际剩余行数校准一次台账，清理与接纳控制按实际值继续 |
| 重复 span 只记一次 | `(trace_id, span_id)` 主键 + `INSERT OR IGNORE`，重复不计入 span 数 |
| 迟到 span 不改判定 | 链一旦变为 kept/dropped，后续 span 一律拒收（OTLP partialSuccess 中返回 rejectedSpans） |
| SQLite 持久化 | 所有状态（含 `first_seen_at`/`deadline_at`/待清理台账）在 SQLite WAL 库中，重启沿用原截止时间 |

后台每 50 ms 结算一次到期链，因此**即使没有新流量**，窗口结束也会按时判定并释放名额；
清理循环默认每 100 ms 清一批，积压会随时间自动排空。

窗口内已报错的链，即便报错 span 是迟到者（窗口关闭后才到）也不会留存——迟到 span
根本不会被接纳。

## 接口

- `POST /v1/traces` — OTLP/HTTP JSON（`Content-Type: application/json`，支持 gzip）。
  成功返回 `{}`；有 span 被拒时返回 OTLP 规范形状：
  `{"partialSuccess": {"rejectedSpans": N, "errorMessage": "..."}}`（HTTP 200）。
  拒绝原因含 `trace capacity full`（并发满）、`cleanup backlog full`（清理积压满）、
  `collection window closed`（迟到）。非 JSON / 结构错误返回 400。
- `GET /api/traces?state=collecting|kept|dropped&limit=50` — 列表 + 状态计数 + 容量 +
  待清理条数（`pending_delete_spans` / `max_pending_delete_spans`）。
- `GET /api/traces/{trace_id}` — 单条链的状态、截止时间、判定原因、留存 span
  （dropped 链不返回原始 span）。
- `GET /api/config`、`GET /api/stats`、`GET /healthz`。
- 网页：`GET /`；`/?trace=<id>` 可直接打开某条链。

### 模拟上报

`POST /api/simulate`，body 例：

```json
{"part": "root", "trace_id": null, "root_duration_ms": 80, "root_error": false,
 "downstream_duration_ms": 40, "downstream_error": false}
```

`part` 取 `both`（一次报整条链）、`root`（只报入口）、`downstream`（补报下游；
需带上 `trace_id`，网页会自动带回）。`count`（1–2000）一次生成多条独立链，
网页的「批量上报制造积压」按钮即用它产生清理积压；`count > 1` 时不能指定
`trace_id`/`span_id`。响应含 `rejected_cleanup`（因积压被拒的 span 数）与
`rejection_message`。模拟器生成两个服务（`checkout-api` 与 `inventory-svc`），
走与 `/v1/traces` 完全相同的解析和入库路径。

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `DB_PATH` | `data/collector.db` | SQLite 路径（Compose 中为 `/data/collector.db`） |
| `COLLECT_WINDOW_SECONDS` | `5` | 收集窗口 |
| `MAX_ACTIVE_TRACES` | `8` | 同时收集的链上限（Compose 演示调为 400） |
| `SLOW_ROOT_MS` | `500` | 慢根阈值（严格大于） |
| `SWEEP_INTERVAL_MS` | `50` | 后台结算间隔 |
| `CLEANUP_BATCH_SIZE` | `200` | 后台清理每次提交最多删除的 span 数 |
| `CLEANUP_INTERVAL_MS` | `100` | 清理批次间隔（Compose 演示放慢为 1000） |
| `MAX_PENDING_DELETE_SPANS` | `1000` | 待清理积压阈值：达到即暂拒新链 |

## 已知限制

- 仅支持 OTLP/HTTP **JSON**，不接收 protobuf（gRPC 未实现）；面对 Go/Python 等
  默认 protobuf 的 exporter，需要设置 `OTEL_EXPORTER_OTLP_PROTOCOL=http/json`。
- 以单进程（单 worker）部署为前提：容量计数与判定靠进程内锁 + `BEGIN IMMEDIATE`
  串行化，多 worker 会各自计容量（SQLite 本身仍一致）。
- 慢根判定基于上报的根 span 自身耗时；若根 span 始终未上报，则只看是否有报错 span。
- 重复 span 按 span_id 去重，保留首次上报的内容。
- dropped 链的元数据行（traces 表）会保留，用于拒收迟到 span 与展示判定结果；
  后台清理只删 span 原文。

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests -q      # 28 个用例，注入时钟覆盖窗口/容量/重启/清理积压等场景
```
