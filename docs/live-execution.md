# 实盘执行层（默认关闭）

应用现在包含实盘下单代码：私钥签名、L2 API 认证、下单、撤单、对账、kill switch。
但**默认只跑纸盘**，执行层不会被自动 pipeline 调用。

## 安全默认

```bash
LAB_TRADING_MODE=paper
LAB_LIVE_ENABLED=0
```

- `paper` 模式：只有纸盘引擎运行，实盘执行层不初始化、不下单
- 实盘 SDK 是可选依赖，不安装不影响纸盘；`/execution` 页面会显示 `SDK missing`
- 私钥和 API 凭据只从环境变量读取，不写数据库、不在页面展示

## 启用实盘的四道门

必须同时满足：

1. `LAB_TRADING_MODE=live`
2. `LAB_LIVE_ENABLED=1`
3. `LAB_LIVE_CONFIRM=I_UNDERSTAND_LIVE_TRADING`
4. 私钥 + L2 API 凭据齐全，且没有激活的 kill switch

Proxy/Safe 钱包还需要：

```bash
LAB_LIVE_SIGNATURE_TYPE=1   # 或 2/3
LAB_LIVE_FUNDER_ADDRESS=0x...
```

### 环境变量

```bash
LAB_TRADING_MODE=live
LAB_LIVE_ENABLED=1
LAB_LIVE_CONFIRM=I_UNDERSTAND_LIVE_TRADING

LAB_LIVE_PRIVATE_KEY=0x...
LAB_LIVE_API_KEY=...
LAB_LIVE_API_SECRET=...
LAB_LIVE_API_PASSPHRASE=...
LAB_LIVE_FUNDER_ADDRESS=0x...     # Proxy/Safe 时必填
LAB_LIVE_SIGNATURE_TYPE=0         # 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE

LAB_LIVE_MAX_ORDER_NOTIONAL_USDC=5
LAB_LIVE_MAX_OPEN_NOTIONAL_USDC=25
LAB_LIVE_MAX_ORDERS_PER_MINUTE=10
LAB_LIVE_MIN_ORDER_SIZE=5
```

### 安装可选 SDK

默认 Docker 镜像不装实盘 SDK。构建时加：

```bash
docker build --build-arg INSTALL_LIVE_DEPS=1 -t polytrader-lab .
```

Coolify 里在 Build Args 增加 `INSTALL_LIVE_DEPS=1`。

## 页面

`/execution`：

- 当前模式、是否 ready、失败原因
- 余额、挂单、历史订单、最近执行事件
- 风控参数
- **刷新对账**
- **取消全部挂单**
- **Kill Switch**（写持久化文件 + DB，取消所有挂单，阻止新订单）
- **解除 kill switch**：必须输入 `RELEASE_KILL_SWITCH`

## 代码接口

`app/services/live.py` 的 `LiveExecutor`：

```bash
# 手动下单 API（需要登录；confirm 固定）
curl -X POST https://your-host/api/live/order \
  -H "Content-Type: application/json" \
  -d '{"token_id":"...","side":"BUY","price":0.55,"size":10,"confirm":"PLACE_LIVE_ORDER"}'
```

```python
await executor.place_limit(token_id, "BUY", 0.55, 10)
await executor.cancel_order(order_id)
await executor.cancel_all("manual")
await executor.reconcile()
await executor.engage_kill("manual")
```

风控在 `_validate_order`：

- 价格必须在 (0,1)
- size ≥ CLOB 最小 5 shares
- 单笔名义 ≤ `LAB_LIVE_MAX_ORDER_NOTIONAL_USDC`
- 未平挂单名义 + 新单 ≤ `LAB_LIVE_MAX_OPEN_NOTIONAL_USDC`
- 每分钟订单数 ≤ 上限
- kill switch / live 四道门 / 凭据必须全部满足

下单使用 **post-only GTC limit**，避免误吃单；当前不实现 GTD/heartbeat。

## 对账

`reconcile()`：

- 拉取 CLOB open orders，更新本地 `live_orders`
- 本地 LIVE/PLACED 但 CLOB 不存在的订单标记 `MISSING`
- 查询 pUSD 余额/allowance，写入 `live_state`
- 事件写入 `live_events`
- 查询失败时不会把本地订单误标为已取消

## 未做的事（重要）

- 执行层**没有接入策略自动下单**。纸盘 pipeline 仍是唯一自动策略运行器。
- 没有用户 WebSocket 实时 fill 推送；对账目前以 open orders + 余额为主，fill 明细后续可加。
- 没有 GTD/heartbeat；使用 GTC + 显式撤单。
- 没有实盘 pilot 的完整风控（订单队列、延迟、撤单确认、日内亏损熔断需要继续完善）。
- 私钥安全、服务器访问控制、提款权限、法律/地区合规由运营方负责。

## 建议流程

1. 纸盘按 14 天 / 2,500+ 结算、t>2、PF>1.2、峰值回撤<10% 通过
2. 安装 live SDK，配置凭据，但保持 `LAB_LIVE_ENABLED=0`，先验证 `/execution` 页面和 kill switch
3. 用 $1–5/单的极小规模手动 pilot，逐笔对账
4. 确认成交率/滑点/返佣与纸盘一致后，再考虑接入策略自动下单
