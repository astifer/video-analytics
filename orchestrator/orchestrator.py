from fastapi import FastAPI, HTTPException

from pydantic import BaseModel
from enum import Enum
from uuid import uuid4
from typing import Dict

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.scenario_models import Scenario, ScenarioUpdate, ScenarioCreate, ScenarioStatus, PredictionResult
from shared.status_models import is_transition_allowed
from shared.utils import get_urls, Settings
from sqlalchemy.ext.asyncio import create_async_engine
from shared.transactional_outbox import OutboxManager, MessageType

from contextlib import asynccontextmanager

import logging
import aiohttp
import asyncio

import time

producer = KafkaProducerWrapper()
consumer = KafkaConsumerWrapper(topic='api-to-orch-commands', group_id="orch-group")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global session, outbox_manager, outbox_processor

    # Initialize HTTP session
    session = aiohttp.ClientSession()

    # Initialize Kafka components
    await producer.start()
    await consumer.start()

    # Initialize Outbox components
    outbox_manager = OutboxManager(settings.db_url, kafka_producer=producer)
    
    # Start the outbox processing loop
    asyncio.create_task(outbox_manager.start_processing_loop())

    yield

    await producer.stop()
    await consumer.stop()
    await session.close()


public_urls = get_urls()
settings = Settings()

RUNNER_URL = public_urls.get("RUNNER_URL")

logger = logging.getLogger(__name__)
app = FastAPI(title="Orchestrator Service", lifespan=lifespan)

scenarios: Dict[str, Scenario] = {}
predictions: Dict[str, PredictionResult] = {}

engine = create_async_engine(
    settings.db_url,
    pool_size=5,
    max_overflow=2,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=True,
)


@app.post("/create_scenario/", response_model=Scenario)
async def create_scenario():
    scenario_id = str(uuid4())
    scenario = Scenario(id=scenario_id, status=ScenarioStatus.init_startup)
    scenarios[scenario_id] = scenario
    
    # Save the scenario creation event to outbox for API
    await outbox_manager.save_message(
        message_type=MessageType.SCENARIO_CREATED,
        payload={"scenario_id": scenario_id, "status": scenario.status},
        target_service='api'
    )
    
    return scenario

@app.post("/scenario/{scenario_id}/", response_model=Scenario)
async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail="Scenario not found")
    
    current_status = scenarios[scenario_id].status
    new_status = update.new_status

    if new_status == current_status:
        raise HTTPException(
            status_code=302,
            detail=f"Scenario already has this status"
        )
    
    if not is_transition_allowed(current_status, new_status):
        raise HTTPException(
            status_code=400,
            detail=f"Transition from {current_status} to {new_status} is not allowed."
        )
    
    # Update scenario status
    scenarios[scenario_id].status = new_status
    
    # Save the status update to outbox for API
    await outbox_manager.save_message(
        message_type=MessageType.SCENARIO_STATUS_UPDATED,
        payload={
            "scenario_id": scenario_id,
            "old_status": current_status,
            "new_status": new_status
        },
        target_service='api'
    )

    if new_status == ScenarioStatus.active:
        # Save the process stream request to outbox for Runner
        await outbox_manager.save_message(
            message_type=MessageType.RUNNER_PROCESS_STREAM,
            payload={"scenario_id": scenario_id},
            target_service='runner'
        )
        
        async with session.post(f"{RUNNER_URL}/process-stream/") as response:
            predictions = await response.json()
            scenarios[scenario_id].data = predictions
            
            # Save the prediction results to outbox for API
            await outbox_manager.save_message(
                message_type=MessageType.PREDICTION_RESULTS,
                payload={
                    "scenario_id": scenario_id,
                    "predictions": predictions
                },
                target_service='api'
            )
            
            return scenarios[scenario_id]

@app.get("/scenario/{scenario_id}/", response_model=Scenario)
async def get_scenario(scenario_id: str):
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return scenarios[scenario_id]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=1612, reload=True)