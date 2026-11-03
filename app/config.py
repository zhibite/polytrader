from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings, all overridable with LAB_* environment variables."""

    app_name: str = "Trader Strategy Lab"
    app_version: str = "0.1.0"

    data_dir: Path = Path("data")
    db_path: Path | None = None

    target_address: str = "0xcd30457c790d8b35a08bcf3f894ad6bb52bc2dd0"

    default_window_days: int = 30
    ingest_max_trades: int = 130_000
    enrich_max_trades: int = 40_000
    enrich_days: int = 14
    request_rate: float = 2.5
    kline_rate: float = 5.0

    klines_dataset_dir: str | None = None

    collector_enabled: bool = True
    collector_asset: str = "btc"
    collector_interval: float = 5.0
    target_poll_seconds: float = 20.0

    monitor_interval_hours: float = 6.0
    monitor_window_days: int = 7

    # Automated research pipeline: ingest -> 3 sweep rounds -> paper strategy.
    auto_pipeline_hours: float = 6.0
    auto_pipeline_days: int = 7
    auto_pipeline_rounds: int = 3
    auto_pipeline_top_k: int = 8
    auto_pipeline_max_cost_per_market: float = 25.0
    auto_pipeline_max_open_cost: float = 500.0

    # Forward paper trading on live collector data.
    paper_enabled: bool = True
    paper_tick_seconds: float = 10.0
    paper_starting_balance: float = 500.0
    paper_min_retention_hours: float = 24.0
    paper_min_settled_trades: int = 100
    paper_promotion_margin: float = 0.01

    demo_seed: bool = False

    api_base: str = "https://data-api.polymarket.com/v2"
    gamma_base: str = "https://gamma-api.polymarket.com"
    clob_base: str = "https://clob.polymarket.com"
    clob_ws: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    future_news_base: str = "https://future.news/api/v1"

    timezone: str = "UTC"

    # Web login credentials. The legacy LAB_BASIC_AUTH_* names are still
    # accepted as a fallback so existing deployments keep working.
    auth_user: str | None = None
    auth_password: str | None = None
    basic_auth_user: str | None = None
    basic_auth_password: str | None = None

    # Signed session cookie for the web login page. If unset, it is derived
    # from the login password (sessions invalidate when the password changes).
    session_secret: str | None = None
    cookie_secure: bool = False

    model_config = SettingsConfigDict(
        env_prefix="LAB_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def database_path(self) -> Path:
        if self.db_path is not None:
            return Path(self.db_path)
        return Path(self.data_dir) / "lab.sqlite"

    @property
    def templates_dir(self) -> Path:
        return Path(__file__).parent / "templates"

    @property
    def static_dir(self) -> Path:
        return Path(__file__).parent / "static"

    def ensure_dirs(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def is_docker(self) -> bool:
        return os.path.exists("/.dockerenv")

    @property
    def login_user(self) -> str | None:
        return self.auth_user or self.basic_auth_user

    @property
    def login_password(self) -> str | None:
        return self.auth_password or self.basic_auth_password

    @property
    def auth_enabled(self) -> bool:
        return bool(self.login_user and self.login_password)

    @property
    def session_secret_key(self) -> str:
        if self.session_secret:
            return self.session_secret
        # Stable per deployment unless the password changes. Production
        # deployments should set LAB_SESSION_SECRET explicitly.
        return f"{self.login_password or 'dev'}::trader-strategy-lab-session"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_dirs()
    return settings
