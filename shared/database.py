from sqlalchemy import create_engine, Column, Integer, String, JSON
from sqlalchemy import DateTime, Integer, Enum as SQLEnum

from sqlalchemy.orm import declarative_base

import logging

from shared.utils import settings
from shared.status_models import MessageStatus, ScenarioStatus

import time
import datetime

logger = logging.getLogger(__name__)
Base = declarative_base()

# exec to check
# psql -U api_user -d api_db
# SELECT * FROM outbox_messages;

class OutboxMessage(Base):
    __tablename__ = 'outbox_messages'

    id = Column(Integer, primary_key=True)
    message_id = Column(String, nullable=False)
    target = Column(String, default='None')
    payload = Column(JSON, nullable=False) # all info we needed, eg `result` or `type`
    status = Column(SQLEnum(MessageStatus), nullable=False, default=MessageStatus.PENDING)
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.now(tz=settings.time_zone))
    processed_at = Column(DateTime, nullable=True)
    retry_count = Column(Integer, default=3)
    error = Column(String, default='None')
    from_service = Column(String, nullable=False) 
    target_service = Column(String, nullable=False)


class Scenario(Base):
    __tablename__ = 'scenarios'

    id = Column(Integer, primary_key=True)
    scenario_id = Column(Integer, nullable=False)
    payload = Column(JSON, default={}) # all info we needed, eg `result` or `type`
    status = Column(SQLEnum(ScenarioStatus), nullable=False, default=ScenarioStatus.INIT_STARTUP)
    created_at = Column(DateTime, nullable=False, default=datetime.datetime.now(tz=settings.time_zone))
    processed_at = Column(DateTime, nullable=True)


def initialize_database(engine):
    """Create tables if they don't exist"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Table 'outbox_messages' created/verified successfully")
    except Exception as e:
        logger.info(f"Table creation failed: {e}")
        raise

def start_connecting(engine):
    """
    Test connection and initialize table
    """
    success = False
    for i in range(3):
        logger.info(f"Trying to init tables. Attempt: {i}")
        try:
            with engine.connect() as connection:
                logger.info("Database connection successful")
            initialize_database(engine)
            success = True
        except Exception as e:
            logger.info(f"Database initialization failed: {e}")

        if success: break
        time.sleep(3)

    return success