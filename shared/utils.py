import yaml
from typing import Dict
from pydantic_settings import BaseSettings
import os
import pytz

def get_urls() -> Dict[str, str]:
    with open('/app/shared/public_urls.yaml', 'r') as f:
        data = yaml.safe_load(f)

    return data

class Settings(BaseSettings):
    POSTGRES_HOST: str = os.environ.get("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int =  os.environ.get("POSTGRES_PORT", 5432)
    POSTGRES_USER: str =  os.environ.get("POSTGRES_USER", "user")
    POSTGRES_PASSWORD: str =  os.environ.get("POSTGRES_PASSWORD", "none")
    POSTGRES_DB: str =  os.environ.get("POSTGRES_DB", "db_name")
    print(f"FOUND IN ENV: {POSTGRES_HOST=}, {POSTGRES_PORT=}, {POSTGRES_USER=}, {POSTGRES_PASSWORD=}, {POSTGRES_DB=}")

    @property
    def db_url(self):
            return (
    f"postgresql+psycopg2://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
            )
    
    @property
    def time_zone(self):
        return pytz.timezone("Europe/Moscow")
    
    @property
    def kafka_bootstrap_servers(self):
        return "kafka:9092"

    @property
    def public_urls(self):
        return get_urls()

settings = Settings()