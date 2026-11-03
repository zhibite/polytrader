from __future__ import annotations

import math
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from ..db import connect, now_ts, to_json
from .ingest import compute_pair_markets, enrich_slug
from .metrics import build_features
from .profiles import generate_profile

TARGET = "0xcd30457c790d8b35a08bcf3f894ad6bb52bc2dd0"
WALLETS = [
    {
        "address": TARGET,
        "label": "Target · BTC 5m maker",
        "source": "docs/1.md",
        "kind": "maker",
        "markets": 170,
        "fills_per_market": 14,
        "maker": 0.97,
        "both_sides": 0.90,
        "tail": 0.08,
        "rebate_bps": 45,
        "stats": {
            "markets": 21716,
            "fills": 577125,
            "volume": 4_977_732.69,
            "realized": 61_269.17,
            "economic": 83_936.98,
            "rebate": 22_579.82,
            "fees": -870.54,
            "join": 1790676148,
        },
    },
    {
        "address": "0x7a3d4c0f92a1b5e8c6d2f4a9b1c3e5d7f9a2b4c6",
        "label": "Pair-lock bot",
        "source": "demo",
        "kind": "pair",
        "markets": 90,
        "fills_per_market": 8,
        "maker": 0.62,
        "both_sides": 0.96,
        "tail": 0.04,
        "rebate_bps": 12,
        "stats": {
            "markets": 4200,
            "fills": 61000,
            "volume": 920_000,
            "realized": 21_400,
            "economic": 23_200,
            "rebate": 1_900,
            "fees": -310,
            "join": 1789000000,
        },
    },
    {
        "address": "0x1f2e3d4c5b6a79887766554433221100ffeeddcc",
        "label": "Directional taker",
        "source": "demo",
        "kind": "taker",
        "markets": 55,
        "fills_per_market": 5,
        "maker": 0.12,
        "both_sides": 0.18,
        "tail": 0.12,
        "rebate_bps": 0,
        "stats": {
            "markets": 700,
            "fills": 2900,
            "volume": 160_000,
            "realized": 8_200,
            "economic": 7_100,
            "rebate": 0,
            "fees": -520,
            "join": 1791000000,
        },
    },
    {
        "address": "0x9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1b0c",
        "label": "Long-tail buyer",
        "source": "demo",
        "kind": "tail",
        "markets": 40,
        "fills_per_market": 3,
        "maker": 0.40,
        "both_sides": 0.08,
        "tail": 0.72,
        "rebate_bps": 4,
        "stats": {
            "markets": 240,
            "fills": 820,
            "volume": 41_000,
            "realized": -2_900,
            "economic": -2_400,
            "rebate": 40,
            "fees": -60,
            "join": 1788000000,
        },
    },
]


def seed_demo(force: bool = False) -> bool:
    conn = connect()
    try:
        existing = conn.execute(
            "SELECT COUNT(*) AS n FROM wallets WHERE source != 'seed'"
        ).fetchone()["n"]
        has_trades = conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()["n"]
        if (existing or has_trades) and not force:
            return False
        if force:
            for table in (
                "wallets",
                "wallet_stats",
                "pnl_points",
                "leaderboard_snapshots",
                "markets",
                "samples",
                "trades",
                "activity",
                "pair_markets",
                "profiles",
                "metric_snapshots",
                "shadow_runs",
                "shadow_trades",
                "replica_runs",
                "replica_trades",
                "monitor_snapshots",
            ):
                conn.execute(f"DELETE FROM {table}")
            conn.commit()
    finally:
        conn.close()
    random.seed(7)
    now = now_ts()
    for config in WALLETS:
        _seed_wallet(config, now)
    _seed_leaderboard(now)
    conn = connect()
    try:
        for config in WALLETS:
            generate_profile(conn, config["address"], days=7)
        for config in WALLETS:
            _seed_monitor(conn, config["address"])
    finally:
        conn.close()
    # One shadow and one replica run so the pages have data on first open.
    try:
        from .shadow import run_shadow

        run_shadow(TARGET, days=3, delay_s=0, size_scale=1.0, run_id="demo_shadow")
    except Exception:
        pass
    try:
        from .replica import run_replica

        run_replica("mid_maker", days=2, params={"size": 8, "offset": 0.005}, run_id="demo_replica")
    except Exception:
        pass
    return True


def _seed_wallet(config: dict[str, Any], now: int) -> None:
    address = config["address"]
    kind = config["kind"]
    conn = connect()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO wallets(address, label, source, notes, tracked, created_at, updated_at)
               VALUES(?,?,?,?,1,?,?)""",
            (address, config["label"], config["source"], f"demo {kind}", now - 86400, now),
        )
        stats = config["stats"]
        conn.execute(
            """INSERT OR REPLACE INTO wallet_stats(
                 address, fetched_at, join_date, markets, fills, volume_shares, volume_usdc,
                 realized_pnl, unrealized_pnl, economic_pnl, settled_pnl, trade_pnl,
                 maker_rebate, taker_rebate, reward_income, referral_income, fees,
                 biggest_win, views, raw_json)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                address,
                now,
                stats["join"],
                stats["markets"],
                stats["fills"],
                stats["volume"] * 1.9,
                stats["volume"],
                stats["realized"],
                18.3,
                stats["economic"],
                stats["economic"] * 0.99,
                stats["realized"],
                stats["rebate"],
                69.6,
                0,
                0,
                stats["fees"],
                544.0,
                2853,
                to_json({"demo": True}),
            ),
        )
        _seed_trades_and_samples(conn, config, now)
        _seed_pnl_points(conn, config, now)
        conn.execute("DELETE FROM pair_markets WHERE address=?", (address,))
        compute_pair_markets(conn, address)
    finally:
        conn.close()


def _seed_trades_and_samples(conn, config: dict[str, Any], now: int) -> None:
    address = config["address"]
    n_markets = int(config["markets"])
    maker = float(config["maker"])
    both = float(config["both_sides"])
    tail = float(config["tail"])
    for index in range(n_markets):
        start = now - 7 * 86400 + index * (7 * 86400 // n_markets)
        start = (start // 300) * 300
        slug = f"btc-updown-5m-{start}"
        resolved = "UP" if random.random() < 0.5 else "DOWN"
        condition = "0x" + f"{random.getrandbits(256):064x}"
        up_token = f"7{random.getrandbits(250):0250d}"
        down_token = f"8{random.getrandbits(250):0250d}"
        conn.execute(
            """INSERT OR REPLACE INTO markets(
                 slug, condition_id, title, asset, period, start_epoch, end_epoch,
                 resolved, up_token, down_token, source, updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                slug,
                condition,
                f"Bitcoin Up or Down — demo {datetime.fromtimestamp(start, timezone.utc):%H:%M}",
                "btc",
                "5m",
                start,
                start + 300,
                resolved,
                up_token,
                down_token,
                "demo",
                now,
            ),
        )
        samples = _build_samples(start, resolved)
        conn.executemany(
            "INSERT OR REPLACE INTO samples(slug, ts, up_price, down_price) VALUES(?,?,?,?)",
            [(slug, ts, up, down) for ts, up, down in samples],
        )
        price_at = {ts: (up, down) for ts, up, down in samples}
        sides = ["UP", "DOWN"] if random.random() < both else [resolved]
        fills = max(1, int(random.gauss(config["fills_per_market"], 6)))
        trade_rows = []
        for _ in range(fills):
            side = random.choice(sides)
            elapsed = random.choice(
                [random.randint(0, 29), random.randint(30, 59), random.randint(60, 119),
                 random.randint(120, 179), random.randint(180, 239), random.randint(240, 295)]
            )
            ts = start + elapsed
            up, down = price_at.get((ts // 1) * 1, samples[min(299, elapsed)])
            mid = up if side == "UP" else down
            is_tail = random.random() < tail
            if is_tail:
                mid = random.uniform(0.02, 0.2)
            offset = random.uniform(0.002, 0.012)
            price = max(0.01, mid - offset)
            size = round(random.uniform(4, 60), 2)
            is_taker = 1 if random.random() > maker else 0
            if is_taker:
                price = min(0.99, mid + random.uniform(0.001, 0.01))
            side_label = "Up" if side == "UP" else "Down"
            outcome_index = 0 if side == "UP" else 1
            ts_fill = start + elapsed
            trade_rows.append(
                (address, f"demo-{slug}-{len(trade_rows)}", f"0xdemo{index:06d}{len(trade_rows):04d}",
                 up_token if side == "UP" else down_token, condition, slug, side_label,
                 outcome_index, "BUY" if random.random() < 0.94 else "SELL", price, size,
                 price * size, ts_fill, is_taker, f"Demo market {index}", slug, start, start + 300)
            )
        conn.executemany(
            """INSERT OR REPLACE INTO trades(
                 address, trade_key, transaction_hash, token_id, condition_id, slug, outcome,
                 outcome_index, side, price, size, usdc_size, ts, is_taker, title, event_slug,
                 market_start, market_end)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            trade_rows,
        )
        conn.commit()
        enrich_slug(conn, address, slug)
        # activity: rebate + redeem
        rebate_total = sum(float(row[9]) * float(row[10]) for row in trade_rows) * config["rebate_bps"] / 10000
        conn.execute(
            """INSERT OR REPLACE INTO activity(address, activity_key, ts, type, condition_id,
                 token_id, side, size, usdc_size, price, outcome, outcome_index, title, slug, transaction_hash)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (address, f"rebate-{slug}", start + 300, "MAKER_REBATE", condition, None, None,
             None, rebate_total, None, None, None, None, slug, None),
        )
        winner_size = sum(float(row[10]) for row in trade_rows if row[6] == resolved.title())
        conn.execute(
            """INSERT OR REPLACE INTO activity(address, activity_key, ts, type, condition_id,
                 token_id, side, size, usdc_size, price, outcome, outcome_index, title, slug, transaction_hash)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (address, f"redeem-{slug}", start + 310, "REDEEM", condition, None, None,
             winner_size, winner_size, 1.0, resolved.title(), None, None, slug, None),
        )
    conn.commit()


def _build_samples(start: int, resolved: str) -> list[tuple[int, float, float]]:
    up = 0.5
    drift = 0.0022 if resolved == "UP" else -0.0022
    out = []
    for second in range(300):
        up = max(0.01, min(0.99, up + drift + random.gauss(0, 0.012)))
        if second > 240:
            target = 0.97 if resolved == "UP" else 0.03
            up = up * 0.85 + target * 0.15
        noise = random.gauss(0, 0.004)
        down = max(0.01, min(0.99, 1 - up + noise))
        out.append((start + second, round(up, 4), round(down, 4)))
    return out


def _seed_pnl_points(conn, config: dict[str, Any], now: int) -> None:
    address = config["address"]
    stats = config["stats"]
    hours = 7 * 24
    base = now - hours * 3600
    realized = stats["realized"] * 0.35
    rebate = stats["rebate"] * 0.35
    economic = stats["economic"] * 0.35
    volume = stats["volume"] * 0.35
    fills = stats["fills"] * 0.35
    rows = []
    for hour in range(hours + 1):
        if hour:
            realized += stats["realized"] / hours * random.uniform(0.4, 1.6)
            rebate += stats["rebate"] / hours * random.uniform(0.4, 1.6)
            economic += stats["economic"] / hours * random.uniform(0.4, 1.6)
            volume += stats["volume"] / hours * random.uniform(0.4, 1.6)
            fills += stats["fills"] / hours * random.uniform(0.4, 1.6)
        rows.append((address, base + hour * 3600, realized, 0.0, economic, realized,
                     rebate, 0.0, stats["fees"] * hour / hours, volume, volume * 1.9, int(fills)))
    conn.executemany(
        """INSERT OR REPLACE INTO pnl_points(
             address, ts, realized_pnl, unrealized_pnl, economic_pnl, position_pnl,
             maker_rebate, taker_rebate, fees, volume_usdc, volume_shares, trade_count)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()


def _seed_leaderboard(now: int) -> None:
    conn = connect()
    try:
        addresses = [config["address"] for config in WALLETS] + [
            "0x" + f"{random.getrandbits(160):040x}" for _ in range(24)
        ]
        random.shuffle(addresses)
        for period in ("day", "week", "month", "all"):
            for category in ("overall", "crypto"):
                for sort_by in ("PNL", "VOLUME"):
                    rows = []
                    for rank, address in enumerate(addresses[:24], start=1):
                        pnl = random.uniform(-5000, 60000) / (1 if period == "all" else 8)
                        volume = random.uniform(10_000, 2_000_000) / (1 if period == "all" else 6)
                        rows.append(
                            (
                                now,
                                period,
                                category,
                                sort_by,
                                rank,
                                address,
                                pnl,
                                volume,
                                f"demo-{rank}",
                                "",
                                "",
                                0,
                                to_json({"rank": rank, "user_id": address, "pnl": pnl, "volume": volume}),
                            )
                        )
                    if sort_by == "PNL":
                        rows.sort(key=lambda row: row[6], reverse=True)
                    else:
                        rows.sort(key=lambda row: row[7], reverse=True)
                    rows = [(row[0], row[1], row[2], row[3], index, *row[5:]) for index, row in enumerate(rows, start=1)]
                    conn.executemany(
                        """INSERT INTO leaderboard_snapshots(
                             captured_at, time_period, category, sort_by, rank, address, pnl,
                             volume, user_name, profile_image, x_username, verified, raw_json)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        rows,
                    )
        conn.commit()
    finally:
        conn.close()


def _seed_monitor(conn, address: str) -> None:
    try:
        features = build_features(conn, address, 7)
        now = now_ts()
        previous = json_clone(features)
        previous["execution"]["entry_edge_mean"] = (previous["execution"].get("entry_edge_mean") or 0) * 1.2
        previous["execution"]["markout_end_mean"] = (previous["execution"].get("markout_end_mean") or 0) * 1.15
        conn.execute(
            "INSERT INTO monitor_snapshots(address, captured_at, window_days, metrics_json) VALUES(?,?,?,?)",
            (address, now - 86400, 7, to_json(previous)),
        )
        conn.execute(
            "INSERT INTO monitor_snapshots(address, captured_at, window_days, metrics_json) VALUES(?,?,?,?)",
            (address, now, 7, to_json(features)),
        )
        conn.commit()
    except Exception:
        pass


def json_clone(value: Any) -> Any:
    import json

    return json.loads(json.dumps(value, default=str))


if __name__ == "__main__":
    print("seeded" if seed_demo(force=True) else "already seeded")
