import yaml
from typing import Dict
from pydantic_settings import BaseSettings

def get_urls() -> Dict[str, str]:
    with open('/app/shared/public_urls.yaml', 'r') as f:
        data = yaml.safe_load(f)

    return data

class Settings(BaseSettings):
    POSTGRESQL_HOST: str
    POSTGRESQL_PORT: int = 5432
    POSTGRESQL_USER: str
    POSTGRESQL_PASSWORD: str
    POSTGRESQL_DBNAME: str

    @property
    def db_url(self):
        return (
f"postgresql+asyncpg://{self.POSTGRESQL_USER}:{self.POSTGRESQL_PASSWORD}@localhost:{self.POSTGRESQL_PORT}/{self.POSTGRESQL_DBNAME}"
        )

    class Config:
        env_file = ".env"