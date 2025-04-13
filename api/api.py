from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
import aiohttp
import logging

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.scenario_models import ScenarioStatus, ScenarioCreate, ScenarioUpdate
from shared.utils import get_urls

from contextlib import asynccontextmanager

import asyncio

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize Kafka producer and consumer
    await producer.start()
    await consumer.start()

    # Start background task to consume messages
    consume_task = asyncio.create_task(consume_commands())

    yield

    await producer.stop()
    await consumer.stop()
    consume_task.cancel()
    try:
        await consume_task
    except asyncio.CancelledError:
        pass


async def consume_commands():
    async for message in consumer.get_messages():
        print(f"[Orchestrator] Received command from API: {message.value}")

producer = KafkaProducerWrapper()
consumer = KafkaConsumerWrapper('orch-to-api-commands', 'api')

public_urls = get_urls()
ORCHESTRATOR_URL = public_urls.get("ORCHESTRATOR_URL")

logger = logging.getLogger(__name__)
app = FastAPI(title="Video Analytics API", lifespan=lifespan)


@app.post("/create_scenario/")
async def create_scenario():

    producer.send('api-to-orch-commands', {'value': 'new'}, key='create_scenario')

    async with aiohttp.ClientSession() as session:
        async with session.post(f"{ORCHESTRATOR_URL}/scenario/", json={}) as response:
            res = await response.json()
            if response.status != 200:
                logger.error(f"[create_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
                raise HTTPException(status_code=response.status, detail=await response.text())
            return res

@app.post("/scenario/{scenario_id}/")
async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/", json=update.model_dump()) as response:
            res = await response.json()
            if response.status != 200:
                logger.error(f"[update_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
                raise HTTPException(status_code=response.status, detail=await response.text())
            return res

@app.get("/scenario/{scenario_id}/")
async def get_scenario(scenario_id: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/") as response:
            res = await response.json()
            if response.status != 200:
                logger.error(f"[get_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
                raise HTTPException(status_code=response.status, detail=await response.text())
            return res

@app.get("/prediction/{scenario_id}/")
async def get_prediction(scenario_id: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{ORCHESTRATOR_URL}/prediction/{scenario_id}/") as response:
            res = await response.json()
            if response.status != 200:
                logger.error(f"[get_prediction] response from ORCHESTRATOR is not OK: {await response.text()}")
                raise HTTPException(status_code=response.status, detail=await response.text())
            return res

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)