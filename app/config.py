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

    demo_seed: bool = False

    api_base: str = "https://data-api.polymarket.com/v2"
    gamma_base: str = "https://gamma-api.polymarket.com"
    clob_base: str = "https://clob.polymarket.com"
    clob_ws: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    future_news_base: str = "https://future.news/api/v1"

    timezone: str = "UTC"

    # Optional HTTP Basic Auth for public deployments. Leave both empty for
    # local/trusted use (e.g. behind an SSH tunnel).
    basic_auth_user: str | None = None
    basic_auth_password: str | None = None

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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_dirs()
    return settings
