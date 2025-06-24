from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from uuid import uuid4

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.scenario_models import ScenarioUpdate, ScenarioStatus
from shared.status_models import is_transition_allowed

from shared.database import Scenario, find_scenario

from shared.utils import settings
from shared.transactional_outbox import OutboxManager

from tools import process_messages_from_runner

from contextlib import asynccontextmanager
import random

import aiohttp
import asyncio


producer = KafkaProducerWrapper(is_idepmotent=True)
runner_consumer = KafkaConsumerWrapper(topic='runner-to-orchestrator', group_id="orchestrator")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global session, outbox_manager, Session_db

    # Initialize HTTP session
    session = aiohttp.ClientSession()

    # Initialize database session
    engine = create_engine(settings.db_url, pool_size=20, max_overflow=0)
    Session_db = sessionmaker(bind=engine)

    # Initialize Kafka components
    await producer.start()
    await runner_consumer.start()

    # Initialize Outbox components
    outbox_manager = OutboxManager(settings.db_url, kafka_producer=producer)
    
    # Start the outbox processing loop
    asyncio.create_task(outbox_manager.start_processing_loop())
    asyncio.create_task(runner_consumer.consume(process_messages_from_runner))

    yield

    await producer.stop()
    await runner_consumer.stop()

    await session.close()


public_urls = settings.public_urls
RUNNER_URL = public_urls.get("RUNNER_URL")

app = FastAPI(title="Orchestrator Service", lifespan=lifespan)


@app.post("/scenario/")
async def create_scenario():
    scenario_id = str(uuid4())
    session_db = Session_db()


    scenario = Scenario( 
        id=random.randint(10**4, 10**9),
        scenario_id=scenario_id, 
        status=ScenarioStatus.INIT_STARTUP)
    
    session_db.add(scenario)
    session_db.close()

    message = {
        "payload": {"scenario_id": scenario_id, "status": ScenarioStatus.INIT_STARTUP}
    }
    await outbox_manager.save_message(
        message=message,
        from_service='orchestrator',
        target_service='api'
    )

    return {"status": 200, "details": {"status": ScenarioStatus.INIT_STARTUP, "scenario_id": scenario_id}}

@app.post("/scenario/{scenario_id}/")
async def update_scenario(scenario_id: str, update: ScenarioUpdate):

    session_db = Session_db()
    scenario = find_scenario(session_db, scenario_id, close_session=False)

    if not scenario:
        print(f"Asking for update for non existing scenario! {scenario_id}")
        raise HTTPException(status_code=404, detail="Scenario not found")
    
    current_status = scenario.status
    new_status = update.new_status

    if new_status == current_status:
        print(f"Scenario already has this status! {scenario_id}")
        raise HTTPException(status_code=302, detail=f"Scenario already has this status")
    
    if not is_transition_allowed(current_status, new_status):
        print(f"Transition from {current_status} to {new_status} is not allowed.")
        raise HTTPException(status_code=400, detail=f"Transition from {current_status} to {new_status} is not allowed")
    
    scenario.status = new_status
    session_db.commit()
    session_db.close()


    message = { 
        "payload": { 
            "scenario_id": scenario_id,
            "status": new_status
        }
    }
    if new_status == ScenarioStatus.IN_STARTUP_PROCESSING:
        print(f"Sending to runner with ask for preprocess")
        message['payload']['target'] = "preprocess"
        message['target'] = "preprocess"
    if new_status == ScenarioStatus.ACTIVE:
        print(f"Sending to runner with ask for inference")
        message['payload']['target'] = "inference"
        message['target'] = "inference"

    await outbox_manager.save_message(
        message=message,
        target_service='runner',
        from_service='orchestrator'
    )  

    return {"status": 200, "details": {"scenario_id": scenario_id, "status": scenario.status}}


@app.get("/scenario/{scenario_id}/")
async def get_scenario(scenario_id: str):
    session_db = Session_db()
    scenario = find_scenario(session_db, scenario_id, close_session=True)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    
    details = scenario.__dict__
    del details['_sa_instance_state']

    return {"status": 200, "details": details}

