# 部署指南（Coolify / Docker）

应用是单进程 FastAPI + SQLite，所有数据写在一个持久化目录 `/app/data`。
采集器与 ingest 任务需要服务器能出站访问 Polymarket；本地网络不通时只在服务器运行。

## 一、Coolify 部署

1. 把本目录推到 Git 仓库（GitHub / Gitee / 自建 Git 均可）。
2. Coolify → **New Resource** → **Application** → 选择仓库。
3. Build Pack 选择 **Dockerfile**（仓库根目录已有 `Dockerfile`）。
4. 端口设为 `8021`（与 Dockerfile 的 `PORT` / `EXPOSE` 一致）。
5. **Persistent Storage**：
   - Volume mount path：`/app/data`
   - 建议使用 named volume 或宿主机目录，例如 `/data/polytrader-lab:/app/data`
6. Environment Variables 至少设置：

```bash
LAB_TARGET_ADDRESS=0xcd30457c790d8b35a08bcf3f894ad6bb52bc2dd0
LAB_COLLECTOR_ENABLED=1
LAB_DEMO_SEED=0
LAB_DEFAULT_WINDOW_DAYS=30
LAB_INGEST_MAX_TRADES=130000
LAB_ENRICH_MAX_TRADES=40000

# 公网部署强烈建议开启 Web 登录
LAB_AUTH_USER=trader
LAB_AUTH_PASSWORD=<long-random-password>
LAB_SESSION_SECRET=<long-random-secret>
LAB_COOKIE_SECURE=1
# 兼容旧配置：未设置 LAB_AUTH_* 时会回退到 LAB_BASIC_AUTH_*
```

7. Health check：
   - Path：`/api/health`
   - Port：`8021`
   - `/api/health` 不受登录保护。
8. Deploy。

首次启动后：

- 打开首页；
- 点击「更新目标数据」，或进入目标钱包页运行 ingest；
- 任务队列在 `jobs` 表，页面顶部会显示进度，完成后自动刷新；
- 首次 7 天 / 4 万笔 enrichment 通常需要几分钟到几十分钟，取决于 kline 数据源。

## 二、可选：挂载已有 1 秒 Kline 数据集

服务器上已有按天分区的数据集时，挂载后 enrichment 会快很多，并覆盖 2026-05 至 2026-11-01 的 BTC 5m 历史。

Coolify → Persistent Storage 添加 bind mount：

```text
Host: /home/ubuntu/polymarket_data/dataset
Container: /data/klines
Read only: yes
```

环境变量：

```bash
LAB_KLINES_DATASET_DIR=/data/klines
```

数据集布局：

```text
/data/klines/asset=btc/period=5m/date=2026-10-31/klines.csv.gz
/data/klines/asset=btc/period=5m/date=2026-10-31/markets.csv.gz
```

没有该目录或日期缺失时，应用会自动回退到 `future.news` 的 1 秒 token kline；
再失败则只拿 Gamma 元数据，markout 留空。

> 注意：现有数据集止于 2026-11-01 04:50 UTC。之后的数据由 future.news
> 回退或 collector 实时采集覆盖。若需要更完整的历史，把新的抓取结果合并进同一
> 数据集目录即可。

## 三、单独用 Docker Compose

```bash
cp .env.example .env
# 编辑 .env
docker compose up -d --build
docker compose logs -f
```

`docker-compose.yml` 已包含 `lab_data:/app/data` 命名卷。若要挂载数据集，取消注释：

```yaml
volumes:
  - lab_data:/app/data
  - /home/ubuntu/polymarket_data/dataset:/data/klines:ro
environment:
  - LAB_KLINES_DATASET_DIR=/data/klines
```

## 四、资源建议

| 资源 | 最低 | 建议 |
|---|---|---|
| CPU | 1 vCPU | 2 vCPU |
| 内存 | 768 MB | 2 GB |
| 磁盘 | 2 GB | 10 GB+（全量成交 + 样本 + collector 数据） |

SQLite 使用 WAL。应用必须以 **单 uvicorn worker** 运行（Dockerfile 已固定
`--workers 1`），否则 job worker 会重复消费队列，且 SQLite 写入竞争增加。

## 五、备份

数据全部在 `/app/data/lab.sqlite`（WAL 模式，另有 `-wal` / `-shm`）。
推荐用 SQLite 在线备份而不是直接 `cp`：

```bash
# 在容器内
python - <<'PY'
import sqlite3
src = sqlite3.connect("/app/data/lab.sqlite")
dst = sqlite3.connect("/app/data/lab-backup.sqlite")
src.backup(dst)
dst.close(); src.close()
PY
```

或从宿主机对 volume 做快照。备份文件可下载后本地打开。

## 六、安全

- 应用默认**没有用户系统**。公网部署务必设置 `LAB_AUTH_USER` / `LAB_AUTH_PASSWORD`；旧版 `LAB_BASIC_AUTH_*` 仍兼容。
- 浏览器使用 `/login` 网页登录（签名 session cookie）；脚本/API 仍可用同一组凭据走 HTTP Basic Auth。
- 更稳妥的做法：只通过 Coolify/Traefik 的访问控制、VPN 或 SSH 隧道暴露。
- 应用只读公开成交与行情，不下单、不签名、不接触私钥。
- 不要在环境变量里放 Polymarket API Key；本项目不需要任何交易凭证。

## 七、常见问题

| 现象 | 排查 |
|---|---|
| `/api/health` 正常但页面跳转 `/login` | 正常：浏览器需要网页登录；脚本请带 Basic Auth |
| ingest 卡在 `fetching all fills` | Data API 限速；降低 `LAB_REQUEST_RATE`（如 1.5），等待退避 |
| 任务重启后从 0 开始 | `ingest_checkpoints` 已清理（成功完成）或 `days` 变了；断点只在同 `(address, days, stream)` 下生效 |
| 大量成交没有 markout | 该日期没有挂载数据集，future.news 也拿不到样本；检查 `LAB_KLINES_DATASET_DIR` / 网络 |
| collector 一直 `未连接` | 服务器出站 WSS 被阻断，或当前 5m 市场 Gamma 尚未索引；看 `/collector` 的 `last_error` |
| 容器启动后 DB 不可写 | 持久化卷权限；让卷由容器内 `appuser` 可写，或用 Coolify 的 volume 初始化 |
| 想清空数据 | 停止容器，删除卷内 `lab.sqlite*`，重新部署；或设置 `LAB_DEMO_SEED=1` 前先备份 |

## 八、升级

重新构建/部署即可。schema 使用 `CREATE TABLE IF NOT EXISTS`，现有 DB 不会被清空；
新增表会在启动时自动创建。升级前建议先做一次 SQLite 备份。
