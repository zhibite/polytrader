from __future__ import annotations

from typing import Any


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _pct(value: Any) -> str:
    if value is None:
        return "—"
    return f"{_num(value) * 100:.1f}%"


def _cents(value: Any) -> str:
    if value is None:
        return "—"
    return f"{_num(value) * 100:+.2f}¢"


def _money(value: Any) -> str:
    if value is None:
        return "—"
    value = _num(value)
    return f"{'-' if value < 0 else ''}${abs(value):,.0f}"


def build_insights(features: dict[str, Any]) -> list[dict[str, str]]:
    ov = features.get("overview") or {}
    ex = features.get("execution") or {}
    pair = features.get("pairing") or {}
    rb = features.get("rebates") or {}
    timing = features.get("timing") or {}
    dec = features.get("decomposition") or {}
    horizons = {item["key"]: item for item in (ex.get("horizons") or [])}
    by_window = ex.get("by_window") or {}
    by_bucket = ex.get("by_bucket") or {}
    out: list[dict[str, str]] = []

    maker = _num(ov.get("maker_share"))
    fills = _num(ov.get("fills"))
    gap = ov.get("median_gap")
    if fills:
        out.append(
            {
                "title": "成交角色",
                "tone": "info",
                "body": (
                    f"窗口内 {fills:,.0f} 笔成交，maker 占比 {_pct(maker)}，"
                    f"成交间隔中位 {gap:.0f}s" if gap is not None else
                    f"窗口内 {fills:,.0f} 笔成交，maker 占比 {_pct(maker)}"
                )
                + f"，平均 size {_num(ov.get('avg_size')):.1f} shares。",
            }
        )

    assets = ov.get("assets") or {}
    participation = features.get("market_participation")
    if assets:
        total = sum(_num(value) for value in assets.values()) or 1.0
        top_asset, top_count = max(assets.items(), key=lambda item: _num(item[1]))
        asset_text = f"{top_asset.upper()} 占 {_pct(top_count / total)}"
        if participation is not None:
            asset_text += f"，覆盖约 {_pct(participation)} 的 {top_asset.upper()} 5m 窗口"
        out.append(
            {
                "title": "市场选择",
                "tone": "info",
                "body": (
                    f"成交资产分布：{asset_text}。"
                    + ("高度集中在单一资产/周期，容量与平台规则变化会直接影响结果。"
                       if _num(top_count / total) >= 0.9 else "跨资产分布较分散。")
                ),
            }
        )

    edge = ex.get("entry_edge_mean")
    positive = ex.get("entry_edge_positive")
    if edge is not None:
        out.append(
            {
                "title": "成交价优势",
                "tone": "positive" if _num(edge) > 0 else "negative",
                "body": (
                    f"成交价相对成交时 mid 平均 {_cents(edge)}，{_pct(positive)} 的成交买在 mid 下方。"
                    "这是 maker 挂单的价格改善，不等同于方向判断。"
                ),
            }
        )

    m1 = horizons.get("markout_1s", {}).get("mean")
    m10 = horizons.get("markout_10s", {}).get("mean")
    m60 = horizons.get("markout_60s", {}).get("mean")
    mend = horizons.get("markout_end", {}).get("mean")
    if mend is not None:
        short_negative = _num(m60) < 0
        out.append(
            {
                "title": "逆向选择 vs 结算 edge",
                "tone": "warn" if short_negative else "positive",
                "body": (
                    f"成交后 +1s / +10s / +60s 的 mid 漂移分别为 {_cents(m1)} / {_cents(m10)} / {_cents(m60)}，"
                    f"到结算为 {_cents(mend)}。"
                    + ("短期被逆向选择，但持有到结算后转为正收益，说明收益来自赔率差而不是短线动量。" if short_negative and _num(mend) > 0
                       else "短期与结算方向一致，需结合 Replica 判断是选时还是成交价。")
                ),
            }
        )

    two_sided = pair.get("two_sided_ratio")
    pair_cost = pair.get("weighted_pair_cost")
    below_one = pair.get("share_matched_below_one")
    if two_sided is not None:
        out.append(
            {
                "title": "双边与配对结构",
                "tone": "info",
                "body": (
                    f"{_pct(two_sided)} 的市场同时买过 Up/Down，加权配对成本 {_num(pair_cost):.3f}，"
                    f"仅 {_pct(below_one)} 的匹配份额成本低于 $1。"
                    + ("这说明它不是纯配对套利；双边买更像是做市库存的自然结果。" if _num(pair_cost) >= 1.0
                       else "配对成本低于 $1 的份额占比较高，存在 locked-pair 成分。")
                ),
            }
        )

    redeem = rb.get("redeem_count")
    sells = ov.get("sells")
    hold = features.get("median_hold_min")
    if redeem is not None or sells is not None:
        out.append(
            {
                "title": "退出方式",
                "tone": "info",
                "body": (
                    f"窗口内 REDEEM {redeem or 0:,} 次，主动 SELL {sells or 0:,} 笔；"
                    + (f"持仓到结算中位 {_num(hold):.1f} 分钟。几乎全部持有到结算，不靠短线卖出退出。"
                       if _num(hold) < 60 else f"持仓中位 {_num(hold):.1f} 分钟，存在跨窗口持有。")
                ),
            }
        )

    rebate_share = rb.get("rebate_share_of_economic")
    lifetime_share = rb.get("lifetime_rebate_share")
    bps = rb.get("rebate_bps")
    if rebate_share is not None or lifetime_share is not None:
        out.append(
            {
                "title": "返佣依赖",
                "tone": "warn" if _num(lifetime_share) >= 0.20 else "info",
                "body": (
                    f"窗口返佣 {_money(rb.get('maker_rebate'))}，有效 {_num(bps):.1f} bps；"
                    f"窗口返佣/经济 PnL {_pct(rebate_share)}，全周期 {_pct(lifetime_share)}。"
                    + ("返佣是收益的重要组成，但需要区分它和真实成交 edge 的贡献。" if _num(lifetime_share) >= 0.20
                       else "返佣不是主要收益来源。")
                ),
            }
        )

    if by_window:
        ordered = sorted(by_window.items(), key=lambda item: _num(item[1].get("markout_end")), reverse=True)
        best, worst = ordered[0], ordered[-1]
        out.append(
            {
                "title": "最佳 / 最差窗口",
                "tone": "info",
                "body": (
                    f"到结算 markout 最好的是 {best[0]}（{_cents(best[1].get('markout_end'))}），"
                    f"最差的是 {worst[0]}（{_cents(worst[1].get('markout_end'))}）。"
                    "Replica 的窗口过滤应优先保留前者。"
                ),
            }
        )

    if by_bucket:
        ordered = sorted(by_bucket.items(), key=lambda item: _num(item[1].get("markout_end")), reverse=True)
        best, worst = ordered[0], ordered[-1]
        out.append(
            {
                "title": "最佳 / 最差价格带",
                "tone": "info",
                "body": (
                    f"到结算最好 {best[0]}（{_cents(best[1].get('markout_end'))}），"
                    f"最差 {worst[0]}（{_cents(worst[1].get('markout_end'))}）。"
                    "长尾价格带需要单独控制容量和风险。"
                ),
            }
        )

    hhi = ov.get("hhi")
    if hhi is not None:
        top = ov.get("top_markets") or []
        top_share = ""
        if top and _num(ov.get("volume_usdc")):
            top_share = f"，Top 市场占成交额 {_pct(_num(top[0].get('volume')) / _num(ov.get('volume_usdc')))}"
        out.append(
            {
                "title": "集中度与容量",
                "tone": "info",
                "body": (
                    f"市场集中度 HHI {_num(hhi):.3f}{top_share}；"
                    f"平均每市场 {_num(ov.get('fills_per_market')):.1f} 笔，最大 {ov.get('max_fills_market')} 笔。"
                ),
            }
        )

    lifetime = dec.get("lifetime") or {}
    window = dec.get("window") or {}
    if lifetime:
        out.append(
            {
                "title": "收益拆解（全周期）",
                "tone": "info",
                "body": (
                    f"仓位 PnL {_money(lifetime.get('realized_pnl'))} + maker 返佣 {_money(lifetime.get('maker_rebate'))} "
                    f"+ taker 返佣 {_money(lifetime.get('taker_rebate'))} + 手续费 {_money(lifetime.get('fees'))} "
                    f"≈ 经济 PnL {_money(lifetime.get('economic_pnl'))}（残差 {_money(lifetime.get('residual'))}）。"
                    "这就是判断“钱来自选时、成交价还是返佣”的账面拆解。"
                ),
            }
        )
    return out
