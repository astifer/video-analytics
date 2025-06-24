import os
import json
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
from .utils import settings

from aiokafka.helpers import create_ssl_context
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

async def wait_for_kafka_bootstrap(bootstrap_servers: str, retries: int = 10, delay: float = 2):
    for attempt in range(retries):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            admin.list_topics()
            admin.close()
            print(f"[Kafka check] Seems to kafka is okay")
            return True
        except NoBrokersAvailable:
            print(f"[Kafka check] Attempt {attempt + 1}/{retries}: Kafka not available yet.")
            await asyncio.sleep(delay)
    raise Exception("Kafka bootstrap servers not available after retries.")


class KafkaProducerWrapper:
    def __init__(self, is_idepmotent: bool = False):
        self._producer = None
        self.is_idepmotent = is_idepmotent
        self.logger = logging.getLogger(__name__)
        self.attempt_to_restart = 3
        self.delay = 5


    async def start(self):
        if self._producer is None:
            await wait_for_kafka_bootstrap(settings.kafka_bootstrap_servers)
            for _ in range(self.attempt_to_restart):
                try:
                    self._producer = AIOKafkaProducer(
                        bootstrap_servers=settings.kafka_bootstrap_servers,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                        enable_idempotence=self.is_idepmotent,
                    )
                    await self._producer.start()
                    await self._producer.send('test-topic', 'Kafka is ready!')
                    self.logger.info("Kafka producer started")
                except Exception as ex:
                    self.logger.info(f"Kafka is not ready for Producer, error: {ex}")
                    await asyncio.sleep(self.delay)

    async def send(self, topic: str, value: dict, key: str = None):
        if not self._producer:
            print(f"Not producer while asking for send message. Trying to start...")
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
        self.attempt_to_restart = 3
        self.delay = 5


    async def start(self):
        if self._consumer is None:
            await wait_for_kafka_bootstrap(settings.kafka_bootstrap_servers)
            for _ in range(self.attempt_to_restart):
                try:
                    self._consumer = AIOKafkaConsumer(
                        self.topic,
                        bootstrap_servers=settings.kafka_bootstrap_servers,
                        group_id=self.group_id,
                        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                        auto_offset_reset="earliest",
                        enable_auto_commit=True,
                        session_timeout_ms=30000,          # default 10000
                        heartbeat_interval_ms=3000,        # default 3000
                        max_poll_interval_ms=60000,  
                    )
                    await self._consumer.start()
                    self.logger.info(f"Kafka consumer started for topic '{self.topic}'")
                except Exception as ex:
                    self.logger.error(f"Kafka is not ready for Consumer, error: {ex}")
                    await asyncio.sleep(self.delay)

    async def consume(self, callback):
        """
        Starts an async loop to consume messages and apply the callback function.
        `callback` must be an `async def` that accepts one argument: message.value
        """
        while True:
            if not self._consumer:
                print(f"Not consumer while asking for consume. Trying to start...")
                await self.start()
            async for message in self._consumer:
                self.logger.info(f"Consumed message from topic '{self.topic}': {message.value}")
                await callback(message.value)

            await asyncio.sleep(0.5)

    async def stop(self):
        if self._consumer:
            await self._consumer.stop()
            self.logger.info("Kafka consumer stopped")