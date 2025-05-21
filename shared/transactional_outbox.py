from sqlalchemy import Column, String, JSON, DateTime, Integer, create_engine, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
import asyncio
from typing import Any, Dict, Optional
import logging
import aiohttp
from .status_models import MessageStatus, MessageType

Base = declarative_base()

class OutboxMessage(Base):
    __tablename__ = 'outbox_messages'

    id = Column(Integer, primary_key=True)
    message_type = Column(SQLEnum(MessageType), nullable=False)
    payload = Column(JSON, nullable=False)
    status = Column(SQLEnum(MessageStatus), nullable=False, default=MessageStatus.PENDING)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    retry_count = Column(Integer, default=0)
    error = Column(String, nullable=True)
    result = Column(JSON, nullable=True)
    target_service = Column(String, nullable=False)  # 'api' or 'runner'

class OutboxManager:
    def __init__(self, db_url: str, kafka_producer=None, runner_url: str = None, retry_count: int = 3):
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.kafka_producer = kafka_producer
        self.runner_url = runner_url
        self.logger = logging.getLogger(__name__)
        self.retry_count = retry_count

    async def save_message(self, message_type: MessageType, payload: Dict[str, Any], target_service: str) -> None:
        """Save a message to the outbox table."""
        session = self.Session()
        try:
            message = OutboxMessage(
                message_type=message_type,
                payload=payload,
                status=MessageStatus.PENDING,
                target_service=target_service
            )
            session.add(message)
            session.commit()
            self.logger.info(f"Saved message to outbox: {message_type} for {target_service}")
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error saving message to outbox: {str(e)}")
            raise
        finally:
            session.close()

    async def process_pending_messages(self) -> None:
        """Process all pending messages in the outbox."""
        session = self.Session()
        try:
            pending_messages = session.query(OutboxMessage).filter(
                OutboxMessage.status == MessageStatus.PENDING
            ).all()

            for message in pending_messages:
                try:
                    message.status = MessageStatus.PROCESSING
                    session.commit()

                    if message.target_service == 'api':
                        await self._process_api_message(message, session)
                    elif message.target_service == 'runner':
                        await self._process_runner_message(message, session)
                    else:
                        raise ValueError(f"Unknown target service: {message.target_service}")

                except Exception as e:
                    message.retry_count += 1
                    message.error = str(e)
                    
                    if message.retry_count >= self.retry_count:
                        message.status = MessageStatus.FAILED
                    
                    session.commit()
                    self.logger.error(f"Error processing message {message.id}: {str(e)}")
        finally:
            session.close()

    async def _process_api_message(self, message: OutboxMessage, session) -> None:
        """Process a message for the API service."""
        if not self.kafka_producer:
            raise Exception("Kafka producer not configured")

        await self.kafka_producer.send_message(
            topic='orch-to-api-commands',
            value=json.dumps({
                "message_type": message.message_type,
                "payload": message.payload,
                "result": message.result,
                "status": message.status,
                "error": message.error,
                "retry_count": message.retry_count,
                "created_at": message.created_at.isoformat(),
                "processed_at": message.processed_at.isoformat() if message.processed_at else None
            })
        )
        message.status = MessageStatus.PROCESSED
        message.processed_at = datetime.utcnow()
        session.commit()
        self.logger.info(f"Successfully processed API message {message.id}")

    async def _process_runner_message(self, message: OutboxMessage, session) -> None:
        """Process a message for the Runner service."""
        if not self.kafka_producer:
            raise Exception("Kafka producer not configured")

        await self.kafka_producer.send_message(
            topic='orch-to-runner-commands',
            value=json.dumps({
                "message_type": message.message_type,
                "payload": message.payload,
                "result": message.result,
                "status": message.status,
                "error": message.error,
                "retry_count": message.retry_count,
                "created_at": message.created_at.isoformat(),
                "processed_at": message.processed_at.isoformat() if message.processed_at else None
            })
        )
        message.status = MessageStatus.PROCESSED
        message.processed_at = datetime.utcnow()
        session.commit()
        self.logger.info(f"Successfully processed Runner message {message.id}")

    async def get_scenario_results(self, scenario_id: str) -> Optional[Dict[str, Any]]:
        """Get the latest processed result for a scenario."""
        session = self.Session()
        try:
            message = session.query(OutboxMessage).filter(
                OutboxMessage.message_type == MessageType.RUNNER_PROCESS_STREAM,
                OutboxMessage.payload['scenario_id'] == scenario_id,
                OutboxMessage.status == MessageStatus.PROCESSED
            ).order_by(OutboxMessage.processed_at.desc()).first()
            
            return message.result if message else None
        finally:
            session.close()

    async def start_processing_loop(self, interval_seconds: int = 5) -> None:
        """Start a background task to process pending messages."""
        while True:
            try:
                await self.process_pending_messages()
            except Exception as e:
                self.logger.error(f"Error in processing loop: {str(e)}")
            
            await asyncio.sleep(interval_seconds)