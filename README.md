# OTLP 尾采样收集器（值班排障用）

接收 OTLP/HTTP JSON，把每条 trace 的 span 先收进 5 秒窗口，窗口结束时做尾采样：
**出错的链、或根 span 耗时超过 500 ms 的链整组留存，其余丢弃**。附带网页，可查看
采样状态与留存的 span，并可模拟"入口先报、下游稍后分批上报"。

## 运行

```bash
docker compose up --build
# 打开 http://localhost:8000
```

状态保存在命名卷 `collector-data` 中的 SQLite 文件里；`docker compose restart`
后沿用原截止时间，不会重新计时。

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
| 重复 span 只记一次 | `(trace_id, span_id)` 主键 + `INSERT OR IGNORE`，重复不计入 span 数 |
| 迟到 span 不改判定 | 链一旦变为 kept/dropped，后续 span 一律拒收（OTLP partialSuccess 中返回 rejectedSpans） |
| SQLite 持久化 | 所有状态（含 `first_seen_at`/`deadline_at`）在 SQLite WAL 库中，重启沿用原截止时间 |

后台每 50 ms 结算一次到期链，因此**即使没有新流量**，窗口结束也会按时判定并释放名额。

窗口内已报错的链，即便报错 span 是迟到者（窗口关闭后才到）也不会留存——迟到 span
根本不会被接纳。

## 接口

- `POST /v1/traces` — OTLP/HTTP JSON（`Content-Type: application/json`，支持 gzip）。
  成功返回 `{}`；有 span 被拒时返回 OTLP 规范形状：
  `{"partialSuccess": {"rejectedSpans": N, "errorMessage": "..."}}`（HTTP 200）。
  非 JSON / 结构错误返回 400。
- `GET /api/traces?state=collecting|kept|dropped&limit=50` — 列表 + 状态计数 + 容量。
- `GET /api/traces/{trace_id}` — 单条链的状态、截止时间、判定原因、留存 span。
- `GET /api/config`、`GET /api/stats`、`GET /healthz`。
- 网页：`GET /`；`/?trace=<id>` 可直接打开某条链。

### 模拟上报

`POST /api/simulate`，body 例：

```json
{"part": "root", "trace_id": null, "root_duration_ms": 80, "root_error": false,
 "downstream_duration_ms": 40, "downstream_error": false}
```

`part` 取 `both`（一次报整条链）、`root`（只报入口）、`downstream`（补报下游；
需带上 `trace_id`，网页会自动带回）。模拟器生成两个服务（`checkout-api` 与
`inventory-svc`），走与 `/v1/traces` 完全相同的解析和入库路径。

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `DB_PATH` | `data/collector.db` | SQLite 路径（Compose 中为 `/data/collector.db`） |
| `COLLECT_WINDOW_SECONDS` | `5` | 收集窗口 |
| `MAX_ACTIVE_TRACES` | `8` | 同时收集的链上限 |
| `SLOW_ROOT_MS` | `500` | 慢根阈值（严格大于） |
| `SWEEP_INTERVAL_MS` | `50` | 后台结算间隔 |

## 已知限制

- 仅支持 OTLP/HTTP **JSON**，不接收 protobuf（gRPC 未实现）；面对 Go/Python 等
  默认 protobuf 的 exporter，需要设置 `OTEL_EXPORTER_OTLP_PROTOCOL=http/json`。
- 以单进程（单 worker）部署为前提：容量计数与判定靠进程内锁 + `BEGIN IMMEDIATE`
  串行化，多 worker 会各自计容量（SQLite 本身仍一致）。
- 慢根判定基于上报的根 span 自身耗时；若根 span 始终未上报，则只看是否有报错 span。
- 重复 span 按 span_id 去重，保留首次上报的内容。

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests -q      # 19 个用例，注入时钟覆盖窗口/容量/重启等场景
```
