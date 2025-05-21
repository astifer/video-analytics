from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from enum import Enum
import aiohttp
import logging
import json

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.scenario_models import ScenarioStatus, ScenarioCreate, ScenarioUpdate
from shared.utils import get_urls, Settings
from sqlalchemy.ext.asyncio import create_async_engine

from contextlib import asynccontextmanager

import asyncio

@asynccontextmanager
async def lifespan(app: FastAPI):
    global session

    # Initialize HTTP session
    session = aiohttp.ClientSession()

    # Initialize Kafka components
    await producer.start()
    await consumer.start()

    # Start Kafka consumer loop
    asyncio.create_task(consume_messages())

    yield

    await session.close()
    await producer.stop()
    await consumer.stop()

producer = KafkaProducerWrapper()
consumer = KafkaConsumerWrapper('orch-to-api-commands', 'api')

public_urls = get_urls()
ORCHESTRATOR_URL = public_urls.get("ORCHESTRATOR_URL")

logger = logging.getLogger(__name__)
settings = Settings()

engine = create_async_engine(
    settings.db_url,
    pool_size=5,
    max_overflow=2,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=True,
)

app = FastAPI(title="Video Analytics API", lifespan=lifespan)

async def consume_messages():
    """Consume messages from Kafka and process them."""
    while True:
        try:
            message = await consumer.get_message()
            if message:
                await process_message(message)
        except Exception as e:
            logger.error(f"Error consuming message: {str(e)}")
        await asyncio.sleep(0.1)

async def process_message(message):
    """Process a message from the outbox."""
    try:
        data = json.loads(message.value)
        message_type = data.get('message_type')
        payload = data.get('payload')
        
        if message_type == 'scenario_created':
            # Handle scenario creation notification
            logger.info(f"Received scenario creation notification: {payload}")
        elif message_type == 'scenario_status_updated':
            # Handle scenario status update notification
            logger.info(f"Received scenario status update: {payload}")
        elif message_type == 'prediction_results':
            # Handle prediction results notification
            logger.info(f"Received prediction results: {payload}")
        else:
            logger.warning(f"Unknown message type: {message_type}")
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")

@app.post("/create_scenario/")
async def create_scenario():
    # Send command to orchestrator
    await producer.send_message(
        topic='api-to-orch-commands',
        value=json.dumps({'command': 'create_scenario'})
    )

    # Wait for response from orchestrator
    async with session.post(f"{ORCHESTRATOR_URL}/create_scenario/") as response:
        res = await response.json()
        if response.status != 200:
            logger.error(f"[create_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
            raise HTTPException(status_code=response.status, detail=await response.text())
        return res

@app.post("/scenario/{scenario_id}/")
async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    # Send command to orchestrator
    await producer.send_message(
        topic='api-to-orch-commands',
        value=json.dumps({
            'command': 'update_scenario',
            'scenario_id': scenario_id,
            'update': update.model_dump()
        })
    )

    async with session.post(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/", json=update.model_dump()) as response:
        res = await response.json()
        if response.status != 200:
            logger.error(f"[update_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
            raise HTTPException(status_code=response.status, detail=await response.text())
        return res

@app.get("/scenario/{scenario_id}/")
async def get_scenario(scenario_id: str):
    async with session.get(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/") as response:
        res = await response.json()
        if response.status != 200:
            logger.error(f"[get_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
            raise HTTPException(status_code=response.status, detail=await response.text())
        return res

@app.get("/prediction/{scenario_id}/")
async def get_prediction(scenario_id: str):
    async with session.get(f"{ORCHESTRATOR_URL}/prediction/{scenario_id}/") as response:
        res = await response.json()
        if response.status != 200:
            logger.error(f"[get_prediction] response from ORCHESTRATOR is not OK: {await response.text()}")
            raise HTTPException(status_code=response.status, detail=await response.text())
        return res

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)