from fastapi import FastAPI, HTTPException

from pydantic import BaseModel
from enum import Enum
from uuid import uuid4
from typing import Dict

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.scenario_models import Scenario, ScenarioUpdate, ScenarioCreate, ScenarioStatus, PredictionResult
from shared.status_models import is_transition_allowed, MessageType

from shared.utils import get_urls, settings
from shared.transactional_outbox import OutboxManager

from contextlib import asynccontextmanager

import aiohttp
import asyncio


producer = KafkaProducerWrapper()

api_consumer = KafkaConsumerWrapper(topic='api-to-orchestrator', group_id="orchestrator")
runner_consumer = KafkaConsumerWrapper(topic='runner-to-orchestrator', group_id="orchestrator")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global session, outbox_manager

    # Initialize HTTP session
    session = aiohttp.ClientSession()

    # Initialize Kafka components
    await producer.start()
    await api_consumer.start()
    await runner_consumer.start()

    # Initialize Outbox components
    outbox_manager = OutboxManager(settings.db_url, kafka_producer=producer)
    
    # Start the outbox processing loop
    asyncio.create_task(outbox_manager.start_processing_loop())

    # start consuming
    asyncio.create_task(api_consumer.consume(process_messages_from_api))
    asyncio.create_task(runner_consumer.consume(process_messages_from_runner))
    
    yield

    await producer.stop()
    await api_consumer.stop()
    await runner_consumer.stop()

    await session.close()


public_urls = get_urls()

RUNNER_URL = public_urls.get("RUNNER_URL")
 
scenarios: Dict[str, Scenario] = {}
predictions: Dict[str, PredictionResult] = {}

app = FastAPI(title="Orchestrator Service", lifespan=lifespan)

async def process_messages_from_api(message_value: dict):
    """
    Received message from api: {"message_id": "2276172c487111f080316619d9cb34fb", "payload": {"scenario_id": "22761a74487111f080316619d9cb34fb", "target": "init_scenario"}, "created_at": "2025-06-13T16:09:27.366037", "processed_at": "2025-06-13T19:12:06.336889+03:00"}
    """
    print(f"Received message from api: {message_value}")
    payload =  message_value.get("payload", {})
    target = payload.get("target")
    scenario_id = payload.get("scenario_id")

    if target == 'init_scenario':
        await create_scenario(scenario_id)

    elif target == 'update_scenario':
        update = payload.get("update")
        if isinstance(update, dict):
            try:
                update = ScenarioUpdate(**update)
                await update_scenario(scenario_id, update)
            except Exception as e:
                print(f"Cannot cast update to ScenarioUpdate from `update` field in message payload")
        else:
            print(f"`update` field in message payload is not a dict after deserialiser")


async def process_messages_from_runner(message_value):
    print(f"Received message from runner: {message_value}")



async def create_scenario(scenario_id: str = None):
    if scenario_id is None:
        scenario_id = str(uuid4())

    scenario = Scenario(id=scenario_id, status=ScenarioStatus.INIT_STARTUP, data={})
    scenarios[scenario_id] = scenario

    await outbox_manager.save_message(
        payload={"scenario_id": scenario_id, "status": scenario.status},
        target_service='api'
    )


async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    if scenario_id not in scenarios:
        print(f"Asking for update for non existing scenario! {scenario_id}")
        return
    
    current_status = scenarios[scenario_id].status
    new_status = update.new_status

    if new_status == current_status:
        print(f"Scenario already has this status! {scenario_id}")
        return
    
    if not is_transition_allowed(current_status, new_status):
        print(f"Transition from {current_status} to {new_status} is not allowed.")
        return
    
    # Update scenario status
    scenarios[scenario_id].status = new_status
    
    await outbox_manager.save_message(
        message={
            "payload": {
                "scenario_id": scenario_id,
                "target": "scenario_updated",
                "status": new_status
            }
        },
        target_service='api',
        from_service='orchestrator'
    )

    message = { 
        "payload": { 
            "scenario_id": scenario_id,
            "status": new_status
        }
    }
    if new_status == ScenarioStatus.IN_STARTUP_PROCESSING:
        print(f"Sending to runner with ask for preprocess")
        message['payload']['target'] = "preprocess"
    if new_status == ScenarioStatus.ACTIVE:
        print(f"Sending to runner with ask for inference")
        message['payload']['target'] = "inference"

    await outbox_manager.save_message(
        message=message,
        target_service='runner',
        from_service='orchestrator'
    )


@app.get("/scenario/{scenario_id}/", response_model=Scenario)
async def get_scenario(scenario_id: str):
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return scenarios[scenario_id]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=1612, reload=True)