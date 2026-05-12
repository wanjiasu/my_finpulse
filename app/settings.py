from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "finpulse-fastapi-celery"

    REDIS_URL: str
    CELERY_BROKER_URL: str
    CELERY_RESULT_BACKEND: str

    POSTGRES_DSN: str

    TUSHARE_TOKEN: str = ""
    TUSHARE_HTTP_URL: str = "http://101.35.233.113:8020/"


settings = Settings()
