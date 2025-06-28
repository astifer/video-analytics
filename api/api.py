from fastapi import FastAPI
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
import uuid
from typing import Dict, Any
from contextlib import asynccontextmanager
import aiohttp
import asyncio

from shared.kafka_client import KafkaConsumerWrapper
from shared.scenario_models import ScenarioUpdate

from shared.utils import (
    settings, 
    make_async_get_request_with_retry, 
    make_async_post_request_with_retry
)

from models import Scenario as LocalScenario


@asynccontextmanager
async def lifespan(app: FastAPI):
    global session, consumer, scenarios

    # Initialize HTTP session
    session = aiohttp.ClientSession()
    consumer = KafkaConsumerWrapper('orchestrator-to-api', 'api')


    await consumer.start()

    # Start Kafka consumer loop
    asyncio.create_task(consumer.consume(consume_messages_from_orchestrator))

    yield

    await session.close()
    await consumer.stop()

scenarios: Dict[str, LocalScenario] = {}

ORCHESTRATOR_URL = settings.public_urls.get("ORCHESTRATOR_URL")
app = FastAPI(title="Video Analytics API", lifespan=lifespan)

async def consume_messages_from_orchestrator(message_value):
    print(f"Received message: {message_value}")
    if isinstance(message_value, str):
        try:
            message_value = json.loads(message_value)
        except:
            print(f"Error while parsiing message from orchestrator {message_value=}")

    payload =  message_value.get("payload", {})
    scenario_id = payload.get("scenario_id")

    scn = LocalScenario( 
        scenario_id=scenario_id, 
        status=payload.get("status", {}),
        payload=payload,
        prediction=payload.get("prediction", {})    
    )
    scenarios[scenario_id] = scn
    print(f"Succecfully get message from orchestrator and update scenario {scenario_id}")


@app.post("/scenario/")
async def create_scenario():

    response = await make_async_post_request_with_retry(
        session=session,
        url=f"{ORCHESTRATOR_URL}/scenario/"
    )
    if response:
        return {"status": 200, "details": response.get("details")}
    else:
        return {"status": 501}


@app.post("/scenario/{scenario_id}/")
async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    '''
    init_startup, in_startup_processing, active, init_shutdown, in_shutdown_processing, inactive
    '''
    response = await make_async_post_request_with_retry(
        session=session,
        url=f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/",
        json=update.model_dump()
    )
    if response.get("status") == 200:
        return {"status": 200, "scenario_id": scenario_id, "new_status": update.new_status}
    elif response.get("status") == 500:
        return {"status": 501, "details": "Server temporaly unavailable"}
    else:
        return response


@app.get("/scenario/{scenario_id}/")
async def get_scenario(scenario_id: str):

    # scenario = scenarios.get(scenario_id)
    # if scenario:
    #     return {"status": 200, "details": {"status": scenario.status, "scenario_id": scenario_id, "payload": scenario.payload}}

    orchestrator_answer = await make_async_get_request_with_retry(
        session=session,
        url=f"{ORCHESTRATOR_URL}/scenario/{scenario_id}"
    )
    if orchestrator_answer:
        return {"status": 200, "details": orchestrator_answer}
    
    return {"status": 404, "details": "Not found"}



@app.get("/prediction/{scenario_id}/")
async def get_prediction(scenario_id: str):

    # scenario = scenarios.get(scenario_id)
    # if scenario:
    #     return {"status": 200, "details": {"status": scenario.status, "scenario_id": scenario_id, "payload": scenario.payload}}

    orchestrator_answer = await make_async_get_request_with_retry(
        session=session,
        url=f"{ORCHESTRATOR_URL}/scenario/{scenario_id}"
    )
    if orchestrator_answer:
        return {"status": 200, "details": orchestrator_answer}
    
    return {"status": 404, "details": "Not found"}
