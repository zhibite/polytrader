# Trader Strategy Lab

Polymarket 交易者行为归因与策略研究平台。目标不是“跟单”，而是把一个公开钱包的成交拆成可解释的结构：

> 他赚的是**选时**、**成交价**、还是**返佣**？

围绕 `docs/1.md` 的五个模块，应用提供：

| 模块 | 页面 | 能力 |
|---|---|---|
| 自动发现 | `/discover` | 拉取官方 leaderboard 的 top PnL / volume（day/week/month/all × 分类），叠加手工地址 |
| 批量归因 | `/wallets`、`/wallets/{address}` | SQLite 队列 + 限速/重试/断点续跑；逐笔 entry edge、markout、配对成本、双边比例、持仓、返佣、时段/价格带、集中度 |
| 策略画像与聚类 | `/wallets/{address}`、`/compare` | 规则打分分类：HFT 做市 / 方向性 taker / 配对套利 / 长尾买入 / 波段 / 返佣型；多钱包特征矩阵与 KMeans 聚类 |
| Shadow / Replica | `/shadow`、`/replica` | Shadow：同价/延迟复刻目标成交；Replica：用推断的报价偏移、窗口、价格带、库存独立跑纸盘 |
| 策略实验室 | `/lab` | 参数网格扫描、自动多轮细化、train/validation、按验证集 ROI 排名、保存策略配置 |
| 前向纸盘 | `/paper` | 用 collector 实时数据模拟成交，展示策略权益、持仓与成交；不碰私钥、不下真实订单 |
| 持续监控 | `/monitor` | 定时快照，跟踪 entry edge、markout、返佣、配对成本、成交量的漂移与告警 |
| 行情采集 | `/collector` | WebSocket 订阅 BTC 5m CLOB market channel，采集 book/price_change/last_trade_price；同时轮询目标钱包最新成交 |

## 技术栈

- Python 3.11+ / FastAPI / Uvicorn
- Jinja2 服务端渲染 + 原生 JS + HTMX（局部刷新）+ ApexCharts
- SQLite（WAL）；无 Redis / Celery
- `httpx` 调 Polymarket Data API v2 / Gamma / future.news
- `websockets` 订阅 CLOB market channel
- pandas / numpy / scikit-learn（特征与聚类）

前端资源全部本地化（`app/static/vendor`），不依赖 CDN。

## 架构

```text
                       ┌──────────────────────────── 服务器 / Coolify ────────────────────────────┐
Polymarket Data API v2 │  FastAPI (uvicorn, 1 worker)                                              │
Gamma / future.news ◄──┼──  JobWorker (SQLite queue, 串行, 限速/重试/断点续跑)                     │
CLOB WebSocket     ◄──┼──  CollectorManager (asyncio task, BTC 5m book/trade)                     │
                       │        ↓                                                                  │
                       │  data/lab.sqlite  (wallets / trades / samples / pair_markets / profiles)  │
                       │        ↓                                                                  │
                       │  Jinja2 页面 + ApexCharts + HTMX                                          │
                       └───────────────────────────────────────────────────────────────────────────┘
                                           ▲
                                    本地浏览器 / SSH 隧道
```

**为什么必须部署在能访问 Polymarket 的机器上**：应用需要出站访问
`data-api.polymarket.com`、`gamma-api.polymarket.com`、`future.news` 和
`ws-subscriptions-clob.polymarket.com`。本地网络若无法直连，使用 Coolify
部署到服务器即可。

## 本地快速开始（合成数据）

本地没有 Polymarket 出网时，可以用 demo 种子验证全部页面：

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt

LAB_DEMO_SEED=1 \
LAB_COLLECTOR_ENABLED=0 \
LAB_MONITOR_INTERVAL_HOURS=0 \
LAB_DB_PATH=data/demo.sqlite \
uvicorn app.main:app --host 127.0.0.1 --port 8011
```

打开 <http://127.0.0.1:8011>。demo 数据是合成的，只用于 UI 验证，不代表真实目标。

## 生产运行（Docker）

```bash
cp .env.example .env
# 编辑 .env：至少设置 LAB_TARGET_ADDRESS；公网部署请设置 LAB_AUTH_USER / LAB_AUTH_PASSWORD / LAB_SESSION_SECRET
docker compose up -d --build
```

首次打开后，在总览页点击「更新目标数据」或访问目标钱包页运行 ingest：

- 默认窗口 `LAB_DEFAULT_WINDOW_DAYS=30`
- 最大成交 `LAB_INGEST_MAX_TRADES=130000`
- Enrich 上限 `LAB_ENRICH_MAX_TRADES=40000`
- 目标地址 `LAB_TARGET_ADDRESS=0xcd30457c790d8b35a08bcf3f894ad6bb52bc2dd0`

详细的 Coolify 部署步骤、持久化卷、可选 kline 数据集挂载见
[DEPLOYMENT.md](DEPLOYMENT.md)。

## 数据管线

`ingest` 任务（`app/services/ingest.py`）：

1. `user-stats` / `user-volume` / `user-pnl(max)` → 全周期统计与 PnL 序列
2. `/v2/trades?taker_only=true` → 确认 taker 成交
3. `/v2/trades?taker_only=false` → 全部成交（taker + maker），按 trade key 去重
4. `/v2/activity` → 只拉非 TRADE 事件（REDEEM / SPLIT / MERGE / MAKER_REBATE / …）
5. `/v2/positions` → 当前 OPEN / REDEEMABLE 持仓
6. `/v2/resolutions` → 按 condition 批量解析 UP/DOWN
7. Kline enrichment：优先本地 1 秒数据集 → future.news 1 秒 API → Gamma 元数据
8. 逐 slug 计算 `mid_at_fill`、+1s/+10s/+60s/结算 markout、entry edge、窗口、价格带
9. 聚合 per-condition 配对成本 / 锁定利润 / 结算 PnL
10. 规则画像 + 研究结论 + PnL 拆解

### 限速、重试、断点续跑

- 全局最小请求间隔（`LAB_REQUEST_RATE` / `LAB_KLINE_RATE`）
- 429 / 5xx 指数退避，`Retry-After` 优先
- 每页写入 SQLite 后保存 cursor 到 `ingest_checkpoints`
- 同一 `(address, days, stream)` 重启会从断点继续；成功后清理 checkpoint，下一次刷新从最新页开始
- 任务在 `jobs` 表排队，单 worker 串行执行，避免并发打爆限速

### Kline 数据源

| 来源 | 粒度 | 用途 |
|---|---|---|
| 本地数据集 `LAB_KLINES_DATASET_DIR` | 5m 市场 1 秒 | 最快；需要挂载 `asset=btc/period=5m/date=.../klines.csv.gz` |
| `future.news` token_kline | 5m 市场 1 秒 | 数据集缺失时的默认回退（带 cookie/限速） |
| Gamma | 仅元数据 | 无法取 kline 时仍可做配对/规模分析，markout 留空 |
| CLOB WebSocket（collector） | 实时 book/trade | 前向监控与 Shadow 验证 |

没有 kline 的成交仍会进入配对、返佣、时段、集中度统计，只是 markout 为空。

## 指标口径

对每笔 BUY（SELL 取反）：

| 指标 | 定义 |
|---|---|
| `entry_edge` | 成交时 mid − 成交价；>0 表示买在 mid 下方 |
| `markout_1s/10s/60s` | t+h 的 mid − 成交时 mid |
| `markout_end` | 市场结束时 mid − 成交时 mid（容差 15s） |
| `elapsed` / `window` | 成交时间 − 市场 start；0-30s / 30-60s / 1-2m / 2-3m / 3-4m / 4-5m |
| `price_bucket` | `<0.10` … `>=0.90` |
| pair cost | 同一 condition 的 Up 均价 + Down 均价 |
| two-sided | 同时买过 Up 和 Down 的市场占比 |
| locked profit | `min(upShares, downShares) × max(0, 1 − pairCost)` 之和 |
| settlement PnL | 赢的一边 shares − 全部净成本 |
| rebate share | (maker + taker rebate) / 经济 PnL；窗口与全周期分别展示 |

## 策略画像

规则打分（透明、可解释），不是黑箱：

- **HFT 做市**：maker ≥ 90%、每日 ≥ 1000 笔、成交间隔 ≤ 15s
- **返佣型**：返佣 / 经济 PnL ≥ 20%、有效返佣 ≥ 30bps
- **配对套利**：双边市场 ≥ 80%、匹配份额成本 < $1 的占比 ≥ 60%
- **方向性 taker**：taker ≥ 50%、结果方向不平衡 ≥ 60%
- **长尾买入**：≥ 40% 成交在 0.20 以下、低频
- **波段**：持仓中位 ≥ 6h、低频

输出 primary classification、confidence、tags、逐条证据和自然语言 verdict。
多钱包时使用 KMeans（标准化特征）聚类，并在 `/compare` 展示。

## Strategy Lab（我们自己的策略）

`/lab` 不是复刻目标，而是在参数空间里找**我们自己的**配置：

- 策略族：`mid_maker`（默认）/ `pair_lock`
- 默认网格：offset `0.002/0.003/0.005/0.008`、5 组窗口、价格带 `0.30-0.90` / `0.50-0.90`、size `5/10/20`、返佣 `0/45bps`
- 风控约束：单市场最大成本 `$25`、总敞口 `$500`
- 按天切分 train / validation，按 validation ROI 排名
- 自动轮次（默认 3）：第 1 轮粗网格，取 Top K 后围绕最优 offset/窗口/价格带/size/rebate 生成第 2、3 轮细网格
- Top 参数可保存到 `strategies` 表，作为后续前向纸盘/实盘候选

注意：回测使用 1 秒 last price + next-tick 穿越，未模拟队列位置、撤单延迟和腿风险；
train/validation 只是时间切分，不等于未来收益。保存策略后必须再做前向纸盘验证。

## 自动流水线

应用默认每 **6 小时**自动执行一次研究流水线（`LAB_AUTO_PIPELINE_HOURS=6`）：

```text
ingest 最近 7 天成交
  → 自动跑 3 轮参数扫描（Round 1 粗网格，之后取 Top K 细化）
  → 保存最优参数到 strategies
  → 自动激活一个新的前向纸盘策略
```

- 纸盘用 collector 的实时盘口/成交，在 trade print 穿过我们的 bid 时模拟成交，持有到结算；
- 旧纸盘策略只有在运行 ≥24 小时或 ≥100 笔已结算后，且新策略 val ROI 领先 ≥1% 时才替换；否则保留当前 active 策略（新配置仍会保存）；
- candidate val ROI ≤0 时不允许激活/替换；宁可空仓；
- 纸盘权益相对起始资金回撤 ≥10% 时自动置为 inactive（停止开新仓，已有持仓继续结算）；
- 被替换的旧策略会置为 inactive，但未结算持仓仍会继续结算；
- 纸盘只做模拟，不使用私钥、不签名、不下单；
- 自动流水线可在 `/lab` 手动触发；`LAB_AUTO_PIPELINE_HOURS=0` 可关闭。

多轮细化仍然是在同一段历史上做局部搜索，不等于未来收益；纸盘前向结果才是第一道真实检验。

## Shadow / Replica

- **Shadow**：把目标窗口内的 BUY 成交按同价（delay=0）或 t+delay 的 mid 复制，持有到结算。
  对比 Shadow PnL 与目标同批 PnL，回答“照抄成交是否赚钱”。
- **Replica**：
  - `mid_maker`：mid − offset 挂买单，下一根 1 秒价格穿过报价即成交，持有到结算；
  - `pair_lock`：Up + Down 成本 < 1 − minEdge 时同时买入。
  参数包含报价偏移、窗口、价格带、size、费用/返佣。
- 两者都不建模队列位置、撤单延迟、腿风险；结论用于结构比较，不是收益承诺。

## 收益拆解

全周期/窗口分别展示：

```
仓位 PnL + maker rebate + taker rebate + 手续费 ≈ 经济 PnL
```

残差用于提示其他收入/估值差。它直接回答“钱来自选时、成交价还是返佣”。

## 目录

```text
app/
  main.py                 FastAPI 入口 / lifespan / session login + API Basic Auth
  config.py               LAB_* 环境变量
  db.py                   SQLite schema 与连接
  templating.py           Jinja2 filters
  routers/                pages / actions / api
  services/
    polymarket.py         Data API v2 / Gamma / future.news 客户端
    klines.py             数据集 / future.news / Gamma 样本加载
    ingest.py             队列任务、断点续跑、enrichment、配对聚合
    metrics.py            行为、markout、配对、返佣、PnL 拆解
    profiles.py           特征、规则分类、聚类
    insights.py           自然语言研究结论
    shadow.py             Shadow 复算
    replica.py            离线 Replica 模拟
    sweep.py              参数网格扫描 + train/validation + 策略保存
    pipeline.py           ingest → 自动多轮扫描 → 保存/激活纸盘
    paper.py              前向纸盘引擎（collector 实时数据）
    monitor.py            快照与漂移
    collector.py          BTC 5m WebSocket 采集
    discovery.py          leaderboard 同步
    jobs.py               SQLite 任务队列与 worker
    demo.py               本地合成数据
  templates/              Jinja2 页面
  static/                 本地 CSS/JS/ApexCharts/HTMX
data/                     SQLite（Docker 持久化卷）
tests/                    单元测试
```

## 限制

- 只能看到公开成交，看不到挂单、撤单、队列位置和真实延迟；这是“行为等价复刻”，不是 100% 还原。
- markout 依赖对应市场/时段的 1 秒样本；样本缺失时留空，不会伪造。
- 1 秒 mid 代替真实盘口，Replica 成交率通常偏乐观。
- 全量 57 万笔成交的 enrichment 很重；默认只 enrich 最近 N 笔。配对/返佣/时段分析仍覆盖全部已入库成交。
- 不包含实盘下单、签名或私钥；无任何盈利保证。

## 测试

```bash
pytest -q
```
