from sqlalchemy import create_engine

from shared.utils import settings
from shared.database import start_connecting

# Create and check connections to tables, that are in the shared.database: outbox_messages and scenarios

# exec to check in pg_api container

# psql -U api_user -d api_db
# SELECT * FROM outbox_messages;
# SELECT * FROM scenarios;

if __name__ == "__main__":
    print(f'Start creating init tables. Database url={settings.db_url}')

    engine = create_engine(url=settings.db_url, pool_pre_ping=True)
    success = start_connecting(engine)

    if not success:
        exit(1)