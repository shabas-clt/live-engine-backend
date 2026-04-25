from typing import List
from pydantic import EmailStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Server Config
    PORT: int = 8001
    NODE_ENV: str = "development"
    ENVIRONMENT: str = "development"

    # Database (PostgreSQL with TimescaleDB)
    TIMESCALEDB_HOST: str = "localhost"
    TIMESCALEDB_PORT: int = 5432
    TIMESCALEDB_USER: str = "postgres"
    TIMESCALEDB_PASSWORD: str = ""
    TIMESCALEDB_DATABASE: str = "live_engine"

    # Redis
    REDIS_URL: str = ""

    # JWT Auth
    JWT_SECRET: str
    JWT_EXPIRES_IN: str = "24h"

    # Default Super Admin
    DEFAULT_ADMIN_EMAIL: EmailStr
    DEFAULT_ADMIN_PASSWORD: str
    DEFAULT_ADMIN_NAME: str = "Super Admin"

    # CORS
    ADMIN_PANEL_URL: str = "http://localhost:3002"
    MAIN_APP_URL: str = "http://localhost:5001"

    # Initial Tiingo Token
    INITIAL_TIINGO_TOKEN: str = ""

    # Data Collection Settings
    COLLECT_BTC: bool = True
    COLLECT_GOLD: bool = True
    COLLECT_SILVER: bool = True
    COLLECT_INDIAN_STOCKS: bool = False

    # Storage Settings
    STORE_RAW_TICKS: bool = True
    CANDLE_INTERVALS: str = "1m,5m,15m,1h,1d"

    @property
    def allowed_origins(self) -> List[str]:
        return [
            self.ADMIN_PANEL_URL,
            self.MAIN_APP_URL,
            "http://localhost:3000",
            "http://localhost:3001",
            "http://localhost:3002",
            "http://localhost:5001",
        ]

    @property
    def timescaledb_url(self) -> str:
        return f"postgresql://{self.TIMESCALEDB_USER}:{self.TIMESCALEDB_PASSWORD}@{self.TIMESCALEDB_HOST}:{self.TIMESCALEDB_PORT}/{self.TIMESCALEDB_DATABASE}"

    @property
    def candle_intervals_list(self) -> List[str]:
        return [i.strip() for i in self.CANDLE_INTERVALS.split(",")]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
