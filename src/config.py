from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    MOEX_BOARD: str = "TQBR"
    MOEX_TICKERS: str = "SBER,GAZP,LKOH"
    MOEX_POLL_INTERVAL_SECONDS: int = 5

    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_RAW_TRADES: str = "raw_trades"

    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_NAME: str = "marketpulse"
    DB_USER: str = "marketpulse"
    DB_PASSWORD: str = "marketpulse"

    @property
    def tickers_list(self) -> list[str]:
        """MOEX_TICKERS хранится строкой через запятую в .env (простой
        текстовый формат), но по коду удобнее работать со списком."""
        return [t.strip() for t in self.MOEX_TICKERS.split(",")]


@lru_cache()
def get_settings() -> Settings:
    return Settings()
