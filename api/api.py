from fastapi import FastAPI, HTTPException
import logging
import json
import uuid

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from contextlib import asynccontextmanager
import aiohttp
import asyncio

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.scenario_models import ScenarioStatus, ScenarioCreate, ScenarioUpdate, MyMessage

from shared.utils import get_urls, Settings
from shared.transactional_outbox import OutboxManager

logger = logging.getLogger(__name__)
settings = Settings()

producer = KafkaProducerWrapper()
consumer = KafkaConsumerWrapper('orchestrator-to-api', 'api')

@asynccontextmanager
async def lifespan(app: FastAPI):
    global session, outbox_manager

    # Initialize HTTP session
    session = aiohttp.ClientSession()

    outbox_manager = OutboxManager(db_url=settings.db_url, kafka_producer=producer, retry_count=3)

    # Initialize Kafka components
    await producer.start()
    await consumer.start()

    # Start Kafka consumer loop
    # asyncio.create_task(consume_messages())

    yield

    await session.close()
    await producer.stop()
    await consumer.stop()


public_urls = get_urls()
app = FastAPI(title="Video Analytics API", lifespan=lifespan)

@app.post("/scenario/")
async def create_scenario():
    message_id = uuid.uuid1().hex
    scenario_id = uuid.uuid1().hex

    message = MyMessage( 
        message_id=message_id, 
        sender='api', 
        to='orchestrator', 
        scenario=None, 
        payload={"scenario_id": scenario_id, "target": 'init_scenario'}
    )
    await outbox_manager.save_message(message.model_dump())

    return {"status": 200, "scenario_id": scenario_id}

@app.post("/scenario/{scenario_id}/")
async def update_scenario(scenario_id: str, update: ScenarioUpdate):

    message_id = uuid.uuid1().hex
    message = MyMessage( 
        message_id=message_id, 
        sender='api', 
        to='orchestrator', 
        payload= {"scenario_id": scenario_id, "target": 'update_scenario', "update": update.model_dump()}
    )
    await outbox_manager.save_message(message.model_dump())

    return {"status": 200, "scenario_id": scenario_id, "new_status": update.new_status}

@app.get("/scenario/{scenario_id}/")
async def get_scenario(scenario_id: str):
    message_id = uuid.uuid1().hex
    message = MyMessage(
        message_id=message_id,
        sender='api',
        to='orchestrator',
        payload={"scenario_id": scenario_id, "target": 'get_scenario'}
    )
    await outbox_manager.save_message(message.model_dump())

    return {"status": 200}


@app.get("/prediction/{scenario_id}/")
async def get_prediction(scenario_id: str):
    return {"status": 200}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)