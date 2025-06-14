from sqlalchemy import Column, String, JSON, DateTime, Integer,  select, create_engine, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import datetime
import json
import asyncio
from typing import Any, Dict, Optional
import logging
import uuid

from .status_models import MessageStatus
from .scenario_models import Scenario
from .utils import settings
from .kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper


Base = declarative_base()


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
    target_service = Column(String, nullable=False)  # 'api' or 'runner'

class OutboxManager:
    def __init__(self, db_url: str, kafka_producer: KafkaProducerWrapper = None, retry_count: int = 3):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine) # but seems to be already created in db.py
        self.Session = sessionmaker(bind=self.engine)

        self.kafka_producer = kafka_producer
        self.logger = logging.getLogger(__name__)
        self.retry_count = retry_count

    async def save_message(self, message: Dict[str, Any], target_service: str = None, from_service: str = None) -> None:
        """Save a message to the outbox table."""
        session = self.Session()

        message_id = message.get("message_id", None)
        if not message_id:
            message_id = uuid.uuid1().hex
        payload = message.get("payload", {})
        target_service = message.get("target_service") or message.get("to") or target_service
        from_service = message.get("from_service") or message.get("from") or message.get("sender") or from_service

        try:
            message = OutboxMessage(
                message_id=message_id,
                target=message.get("target"),
                payload=payload,
                status=MessageStatus.PENDING,
                from_service=from_service,
                target_service=target_service,
            )
            session.add(message)
            session.commit()
            self.logger.info(f"Saved message to outbox for {target_service}")
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error saving message to outbox: {str(e)}")
            raise
        finally:
            session.close()

    async def start_processing_loop(self, interval_seconds: int = 2) -> None:
        """Start a background task to process pending messages."""
        while True:
            try:
                await self.process_pending_messages()
            except Exception as e:
                self.logger.error(f"Error in processing loop: {str(e)}")
            
            await asyncio.sleep(interval_seconds)

    async def process_pending_messages(self) -> None:
        """Process all pending messages in the outbox."""
        session = self.Session()
        stmt = select(OutboxMessage).filter(
            OutboxMessage.status == MessageStatus.PENDING
        )
        
        result = session.execute(stmt)
        pending_messages = result.scalars().all()

        for message in pending_messages:
            # Nested transactions allow partial success (one failed message doesn't block others)
            try:
                message.status = MessageStatus.PROCESSING
                await self._process_message(message, session)

            except Exception as e:
                # Handle errors with nested transaction
                async with session.begin_nested():
                    message.retry_count += 1
                    message.error = str(e)
                    
                    if message.retry_count >= self.retry_count:
                        message.status = MessageStatus.FAILED
                        message.error = f"Permanent failure: {e.__class__.__name__}"
                    else:
                        message.status = MessageStatus.PENDING  # Retry later
                
                self.logger.error(f"Error processing message {message.id}: {str(e)}")
            finally:
                session.close()


    async def _process_message(self, message: OutboxMessage, session) -> None:
        """Process a message for some service."""
        if not self.kafka_producer:
            raise Exception(f"Kafka producer not configured in {self.__class__.__name__}")
        
        # Validate required fields
        if not message.target_service:
            raise ValueError("Message missing target_service")
        if not message.from_service:
            raise ValueError("Message missing from_service")
        
        # Prepare Kafka message
        topic = f"{message.from_service}-to-{message.target_service}"
        value = {
            "message_id": message.message_id,
            "payload": message.payload,
            "created_at": message.created_at.isoformat(),
            "processed_at": datetime.datetime.now(tz=settings.time_zone).isoformat()
        }

        try:
            await self.kafka_producer.send(topic=topic, value=json.dumps(value))
            message.status = MessageStatus.PROCESSED
            message.processed_at = datetime.datetime.now(tz=settings.time_zone)
       
        except Exception as e:
            self.logger.error(f"Kafka send failed for {topic}: {str(e)}")
            raise  # Re-raise for retry handling
        
        session.commit()