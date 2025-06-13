from sqlalchemy import create_engine, Column, Integer, String, JSON, TIMESTAMP, Text
from sqlalchemy import Column, String, JSON, DateTime, Integer, create_engine, Enum as SQLEnum

from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

import logging

from shared.utils import Settings
from shared.status_models import MessageStatus

import time
import datetime

settings = Settings()

logger = logging.getLogger(__name__)

logger.info(f'Start creating init tables. Database url={settings.db_url}')

engine = create_engine(url=settings.db_url, pool_pre_ping=True)
Base = declarative_base()

# exec to check
# psql -U api_user -d api_db
# SELECT * FROM outbox_messages;

class OutboxMessage(Base):
    __tablename__ = 'outbox_messages'

    id = Column(Integer, primary_key=True)
    message_id = Column(String, nullable=False)
    payload = Column(JSON, nullable=False) # all info we needed, eg `result` or `type`
    status = Column(SQLEnum(MessageStatus), nullable=False, default=MessageStatus.PENDING)
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.now(tz=settings.time_zone))
    processed_at = Column(DateTime, nullable=True)
    retry_count = Column(Integer, default=3)
    error = Column(String, default='None')
    from_service = Column(String, nullable=False) 
    target_service = Column(String, nullable=False)
    

def initialize_database():
    """Create tables if they don't exist"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Table 'outbox_messages' created/verified successfully")
    except Exception as e:
        logger.info(f"Table creation failed: {e}")
        raise

# Test connection and initialize table
if __name__ == "__main__":
    success = False
    for i in range(3):
        logger.info(f"Trying to init tables. Attempt: {i}")
        try:
            with engine.connect() as connection:
                logger.info("Database connection successful")
            initialize_database()
            success = True
        except Exception as e:
            logger.info(f"Database initialization failed: {e}")

        time.sleep(3)

    if not success:
        exit(1)