import os
import json
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class KafkaProducerWrapper:
    def __init__(self):
        self._producer = None

    async def start(self):
        attempt_to_restart = 10
        delay = 3

        if self._producer is None:
            for i in range(attempt_to_restart):
                try:
                    self._producer = AIOKafkaProducer(
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8")
                    )
                    await self._producer.start()
                    logger.info("Kafka producer started")
                except Exception as ex:
                    logger.info(f"Kafka is not ready for Producer, error: {ex}")
                    await asyncio.sleep(delay)

    async def send(self, topic: str, value: dict, key: str = None):
        if not self._producer:
            await self.start()
        key_bytes = key.encode("utf-8") if key else None
        await self._producer.send_and_wait(topic, value=value, key=key_bytes)
        logger.info(f"Sent message to topic '{topic}': {value}")

    async def stop(self):
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")
            self._producer = None


class KafkaConsumerWrapper:
    def __init__(self, topic: str, group_id: str):
        self.topic = topic
        self.group_id = group_id
        self._consumer = None

    async def start(self):
        attempt_to_restart = 10
        delay = 3

        if self._consumer is None:
            for i in range(attempt_to_restart):
                try:
                    self._consumer = AIOKafkaConsumer(
                        self.topic,
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        group_id=self.group_id,
                        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                        auto_offset_reset="earliest",
                        enable_auto_commit=True,
                    )
                    await self._consumer.start()
                    logger.info(f"Kafka consumer started for topic '{self.topic}'")
                except Exception as ex:
                    logger.error(f"Kafka is not ready for Consumer, error: {ex}")
                    asyncio.sleep(delay)

    async def consume(self, callback):
        """
        Starts an async loop to consume messages and apply the callback function.
        `callback` must be an `async def` that accepts one argument: message.value
        """
        if not self._consumer:
            await self.start()
        async for message in self._consumer:
            logger.info(f"Consumed message from topic '{self.topic}': {message.value}")
            await callback(message.value)

    async def stop(self):
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka consumer stopped")
            self._consumer = None
