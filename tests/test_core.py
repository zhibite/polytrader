from __future__ import annotations

import time

import pytest

from app.config import get_settings
from app.db import connect, init_db, now_ts
from app.services.ingest import compute_pair_markets, enrich_slug
from app.services.insights import build_insights
from app.services.polymarket import parse_clob_tokens, parse_slug
from app.services.profiles import classify
from app.services.replica import run_replica

ADDRESS = "0xcd30457c790d8b35a08bcf3f894ad6bb52bc2dd0"


@pytest.fixture()
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("LAB_DB_PATH", str(tmp_path / "test.sqlite"))
    monkeypatch.setenv("LAB_COLLECTOR_ENABLED", "0")
    get_settings.cache_clear()
    init_db()
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()
        get_settings.cache_clear()


def _seed_market(conn, address=ADDRESS, start=None, resolved="UP"):
    start = start or (now_ts() - 3600)
    slug = f"btc-updown-5m-{start}"
    condition = "0x" + "ab" * 32
    conn.execute(
        """INSERT OR REPLACE INTO markets(
             slug, condition_id, title, asset, period, start_epoch, end_epoch,
             resolved, up_token, down_token, source, updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (slug, condition, "BTC demo", "btc", "5m", start, start + 300, resolved,
         "up-token", "down-token", "test", now_ts()),
    )
    samples = []
    for second in range(300):
        up = max(0.01, 0.6 - second * 0.0005)
        down = max(0.01, 0.4 + second * 0.0004)
        samples.append((slug, start + second, up, down))
    conn.executemany(
        "INSERT OR REPLACE INTO samples(slug, ts, up_price, down_price) VALUES(?,?,?,?)",
        samples,
    )
    trades = []
    for index, (outcome, outcome_index, second) in enumerate(
        [("Up", 0, 10), ("Down", 1, 20), ("Up", 0, 30), ("Down", 1, 40)]
    ):
        price = (0.6 - second * 0.0005 - 0.005) if outcome == "Up" else (0.4 + second * 0.0004 - 0.005)
        trades.append(
            (
                address, f"key-{index}", f"tx-{index}", "token", condition, slug, outcome,
                outcome_index, "BUY", price, 10.0, price * 10.0, start + second, 0,
                "BTC demo", slug, start, start + 300,
            )
        )
    conn.executemany(
        """INSERT OR REPLACE INTO trades(
             address, trade_key, transaction_hash, token_id, condition_id, slug, outcome,
             outcome_index, side, price, size, usdc_size, ts, is_taker, title, event_slug,
             market_start, market_end)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        trades,
    )
    conn.commit()
    return slug, condition, start


def test_parse_slug_and_tokens():
    assert parse_slug("btc-updown-5m-1793670300") == ("btc", "5m", 1793670300)
    assert parse_slug("eth-updown-15m-1793670000") == ("eth", "15m", 1793670000)
    assert parse_slug("not-a-market") == (None, None, None)
    up, down = parse_clob_tokens(
        {"clobTokenIds": '["111", "222"]', "outcomes": '["Up", "Down"]'}
    )
    assert (up, down) == ("111", "222")


def test_enrich_and_pair(db):
    slug, condition, start = _seed_market(db)
    n = enrich_slug(db, ADDRESS, slug)
    assert n == 4
    rows = db.execute(
        "SELECT * FROM trades WHERE address=? ORDER BY ts", (ADDRESS,)
    ).fetchall()
    assert all(row["entry_edge"] is not None for row in rows)
    # fill is 0.5c below mid for both sides
    assert all(abs(row["entry_edge"] - 0.005) < 1e-6 for row in rows)
    assert all(row["markout_end"] is not None for row in rows)
    pairs = compute_pair_markets(db, ADDRESS)
    assert pairs == 1
    pair = db.execute("SELECT * FROM pair_markets WHERE address=?", (ADDRESS,)).fetchone()
    assert pair["up_shares"] == pytest.approx(20.0)
    assert pair["down_shares"] == pytest.approx(20.0)
    assert pair["matched_shares"] == pytest.approx(20.0)
    assert pair["pair_cost"] is not None
    assert pair["settlement_pnl"] is not None


def test_classify_target_profile():
    features = {
        "maker_share": 0.97,
        "taker_share": 0.03,
        "fills_per_day": 4000,
        "median_gap_s": 2,
        "two_sided_ratio": 0.90,
        "share_matched_below_one": 0.38,
        "rebate_share_of_economic": 0.001,
        "rebate_share_lifetime": 0.27,
        "rebate_bps": 45,
        "entry_edge_mean": 0.0057,
        "markout_end": 0.0142,
        "median_hold_min": 4.3,
        "long_tail_share": 0.10,
        "outcome_imbalance": 0.05,
        "markets": 200,
        "locked_profit": 10.0,
    }
    result = classify(features)
    assert result["classification"] == "hft_maker"
    assert "rebate_farmer" in [tag for tag in result["tags"]]
    assert 0 < result["confidence"] <= 0.95


def test_insights_are_generated():
    features = {
        "overview": {"fills": 1000, "maker_share": 0.97, "median_gap": 2, "avg_size": 20,
                     "sells": 5, "hhi": 0.01, "fills_per_market": 5, "max_fills_market": 30,
                     "top_markets": [], "volume_usdc": 10000},
        "execution": {"entry_edge_mean": 0.005, "entry_edge_positive": 0.6,
                      "horizons": [{"key": "markout_60s", "mean": -0.002},
                                   {"key": "markout_end", "mean": 0.014}],
                      "by_window": {"0-30s": {"markout_end": 0.02}, "3-4m": {"markout_end": -0.03}},
                      "by_bucket": {"0.50-0.60": {"markout_end": 0.01}}},
        "pairing": {"two_sided_ratio": 0.91, "weighted_pair_cost": 1.046,
                    "share_matched_below_one": 0.377},
        "rebates": {"redeem_count": 500, "maker_rebate": 100, "rebate_bps": 45,
                    "rebate_share_of_economic": 0.27, "lifetime_rebate_share": 0.268},
        "decomposition": {"lifetime": {"realized_pnl": 61000, "maker_rebate": 22500,
                                        "taker_rebate": 70, "fees": -870, "economic_pnl": 83900,
                                        "residual": 1200}},
        "median_hold_min": 4.3,
    }
    insights = build_insights(features)
    assert len(insights) >= 8
    assert all("title" in item and "body" in item for item in insights)


def test_filters_handle_jinja_undefined():
    from jinja2 import Undefined

    from app.templating import cents, money, pct, price

    assert price(Undefined(name="weighted_pair_cost")) == "—"
    assert money(Undefined(name="m")) == "—"
    assert pct(Undefined(name="p")) == "—"
    assert cents(Undefined(name="c")) == "—"


def test_auth_session_helpers(monkeypatch):
    from app.auth import (
        credentials_ok,
        issue_session,
        read_session,
        safe_next,
    )

    monkeypatch.setenv("LAB_AUTH_USER", "alice")
    monkeypatch.setenv("LAB_AUTH_PASSWORD", "secret")
    monkeypatch.setenv("LAB_SESSION_SECRET", "test-secret")
    get_settings.cache_clear()
    try:
        assert credentials_ok("alice", "secret") is True
        assert credentials_ok("alice", "wrong") is False
        token = issue_session("alice")
        assert read_session(token) == "alice"
        assert read_session(token + "x") is None
        assert safe_next("/wallets/0x1?days=7") == "/wallets/0x1?days=7"
        assert safe_next("//evil.example") == "/"
        assert safe_next("https://evil.example") == "/"
    finally:
        get_settings.cache_clear()


def test_collector_parses_price_changes_and_trades(db):
    import json

    from app.services.collector import CollectorManager

    manager = CollectorManager()
    books = {
        "up-token": {"bids": {}, "asks": {}},
        "down-token": {"bids": {}, "asks": {}},
    }
    outcomes = {"up-token": "UP", "down-token": "DOWN"}
    payload = {
        "market": "0xabc",
        "event_type": "price_change",
        "price_changes": [
            {"asset_id": "up-token", "price": "0.50", "size": "100", "side": "BUY"},
            {"asset_id": "up-token", "price": "0.55", "size": "40", "side": "SELL"},
            {"asset_id": "down-token", "price": "0.45", "size": "70", "side": "BUY"},
        ],
        "timestamp": "1793673642498",
    }
    manager._handle_message(db, json.dumps(payload), books, outcomes, "btc-updown-5m-1")
    assert books["up-token"]["bids"][0.50] == 100
    assert books["up-token"]["asks"][0.55] == 40
    assert books["down-token"]["bids"][0.45] == 70

    trade = {
        "market": "0xabc",
        "event_type": "last_trade_price",
        "asset_id": "up-token",
        "price": "0.51",
        "size": "5",
        "side": "BUY",
        "timestamp": "1793673642618",
    }
    manager._handle_message(db, json.dumps(trade), books, outcomes, "btc-updown-5m-1")
    row = db.execute("SELECT * FROM collector_trades").fetchone()
    assert row["price"] == pytest.approx(0.51)
    assert row["outcome"] == "UP"


def test_replica_mid_maker(db):
    slug, condition, start = _seed_market(db)
    result = run_replica(
        "mid_maker",
        days=1,
        params={
            "size": 5,
            "offset": 0.0001,
            "min_price": 0.05,
            "max_price": 0.95,
            "allowed_windows": [],
            "rebate_bps": 0,
        },
        run_id="test-replica",
    )
    assert result["summary"]["trades"] >= 1
    assert result["summary"]["pnl"] is not None
