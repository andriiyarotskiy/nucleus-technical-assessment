from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="APP_",
        extra="ignore",
    )
    app_name: str = "Transaction Event Service"
    environment: Literal["local", "test", "production"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    postgres_dsn: str = (
        "postgresql+asyncpg://postgres:postgres@localhost:5432/transactions"
    )
    redis_url: str = "redis://localhost:6379/0"
    redis_stream_name: str = "transactions"
    redis_consumer_group: str = "transaction-processors"
    redis_consumer_name: str | None = None
    redis_dead_letter_stream: str = "transactions:dead-letter"
    redis_metrics_key: str = "transaction-service:metrics"
    redis_batch_size: int = Field(default=10, ge=1, le=100)
    redis_block_ms: int = Field(default=5_000, ge=1)
    worker_retry_delay_ms: int = Field(default=5_000, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
