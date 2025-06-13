from sqlalchemy import create_engine, Column, Integer, String, JSON, TIMESTAMP, Text
from sqlalchemy import Column, String, JSON, DateTime, Integer, create_engine, Enum as SQLEnum

from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

import logging

from shared.utils import Settings
from shared.status_models import MessageStatus
from shared.transactional_outbox import OutboxMessage

import time
import datetime

settings = Settings()


print(f'Start creating init tables. Database url={settings.db_url}')

engine = create_engine(url=settings.db_url, pool_pre_ping=True)
Base = declarative_base()

# exec to check
# psql -U api_user -d api_db
# SELECT * FROM outbox_messages;

class Scenario(Base):
    __tablename__ = 'api_table'

    id = Column(Integer, primary_key=True)
    payload = Column(JSON, nullable=False) # all info we needed, eg `result` or `type`
    status = Column(SQLEnum(MessageStatus), nullable=False, default=MessageStatus.PENDING)
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.now(tz=settings.time_zone))
    processed_at = Column(DateTime, nullable=True)


def initialize_database():
    """Create tables if they don't exist"""
    try:
        Base.metadata.create_all(bind=engine)
        print("Tables created/verified successfully")
    except Exception as e:
        print(f"Table creation failed: {e}")
        raise

# Test connection and initialize table
if __name__ == "__main__":
    success = False
    for i in range(3):
        print(f"Trying to init tables. Attempt: {i}")
        try:
            with engine.connect() as connection:
                print("Database connection successful")
            initialize_database()
            success = True
        except Exception as e:
            print(f"Database initialization failed: {e}")

        time.sleep(3)

    if not success:
        exit(1)