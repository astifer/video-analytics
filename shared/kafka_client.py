import os
import json
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
from .utils import Settings

settings = Settings()


class KafkaProducerWrapper:
    def __init__(self):
        self._producer = None
        self.logger = logging.getLogger(__name__)

    async def start(self):
        attempt_to_restart = 3
        delay = 3

        if self._producer is None:
            for _ in range(attempt_to_restart):
                try:
                    self._producer = AIOKafkaProducer(
                        bootstrap_servers=settings.kafka_bootstrap_servers,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8")
                    )
                    await self._producer.start()
                    await self._producer.send('test-topic', 'Kafka is ready!')
                    self.logger.info("Kafka producer started")
                except Exception as ex:
                    self.logger.info(f"Kafka is not ready for Producer, error: {ex}")
                    await asyncio.sleep(delay)

    async def send(self, topic: str, value: dict, key: str = None):
        if not self._producer:
            await self.start()
        key_bytes = key.encode("utf-8") if key else None
        await self._producer.send_and_wait(topic, value=value, key=key_bytes)
        self.logger.info(f"Sent message to topic '{topic}': {value}")

    async def stop(self):
        if self._producer:
            await self._producer.stop()
            self.logger.info("Kafka producer stopped")
            self._producer = None


class KafkaConsumerWrapper:
    def __init__(self, topic: str, group_id: str=None):
        self.topic = topic
        self.group_id = group_id
        self._consumer = None
        self.logger = logging.getLogger(__name__)


    async def start(self):
        attempt_to_restart = 3
        delay = 3

        if self._consumer is None:
            for _ in range(attempt_to_restart):
                try:
                    self._consumer = AIOKafkaConsumer(
                        self.topic,
                        bootstrap_servers=settings.kafka_bootstrap_servers,
                        group_id=self.group_id,
                        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                        auto_offset_reset="earliest",
                        enable_auto_commit=True,
                    )
                    await self._consumer.start()
                    self.logger.info(f"Kafka consumer started for topic '{self.topic}'")
                except Exception as ex:
                    self.logger.error(f"Kafka is not ready for Consumer, error: {ex}")
                    await asyncio.sleep(delay)

    async def consume(self, callback):
        """
        Starts an async loop to consume messages and apply the callback function.
        `callback` must be an `async def` that accepts one argument: message.value
        """
        if not self._consumer:
            await self.start()
        async for message in self._consumer:
            self.logger.info(f"Consumed message from topic '{self.topic}': {message.value}")
            await callback(message.value)

    async def stop(self):
        if self._consumer:
            await self._consumer.stop()
            self.logger.info("Kafka consumer stopped")