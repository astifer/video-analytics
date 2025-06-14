from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

import uuid

from contextlib import asynccontextmanager
import aiohttp
import asyncio

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.scenario_models import ScenarioUpdate, MyMessage

from shared.utils import settings
from shared.transactional_outbox import OutboxManager
from shared.database import Scenario

# from database import Scenario

producer = KafkaProducerWrapper()
consumer = KafkaConsumerWrapper('orchestrator-to-api', 'api')


@asynccontextmanager
async def lifespan(app: FastAPI):
    global session, outbox_manager, Session_db

    # Initialize HTTP session
    session = aiohttp.ClientSession()

    # Initialize database session
    engine = create_engine(settings.db_url)
    Session_db = sessionmaker(bind=engine)

    outbox_manager = OutboxManager(db_url=settings.db_url, kafka_producer=producer, retry_count=3)

    # Initialize Kafka components
    await producer.start()
    await consumer.start()

    # Start Kafka consumer loop
    asyncio.create_task(outbox_manager.start_processing_loop())
    asyncio.create_task(consumer.consume(consume_messages_from_orchestrator))

    yield

    await session.close()
    await producer.stop()
    await consumer.stop()

ORCHESTRATOR_URL = settings.public_urls.get("ORCHESTRATOR_URL")
app = FastAPI(title="Video Analytics API", lifespan=lifespan)

async def consume_messages_from_orchestrator(message_value):
    print(f"Received message: {message_value}")

    payload =  message_value.get("payload", {})
    scenario_id = payload.get("scenario_id")

    await update_local_scenario(scenario_id, message_value)


async def update_local_scenario(scenario_id, message_value):
    target = message_value.get("target") or payload.get("target")

    payload =  message_value.get("payload", {})
    status = payload.get("status")

    session_db = Session_db()
    stmt = select(Scenario).filter(
        Scenario.scenario_id == scenario_id
    )
    result = session_db.execute(stmt)
    scenario = result.scalars().first()

    scenario.payload = payload
    if status:
        scenario.status = status

    session_db.commit()
    session_db.close()

async def ask_orchestrator_actual_status(scenario_id):
    for _ in range(3):
        async with session.post(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}") as response:
            res = await response.json()
            if response.status != 200:
                print(f"[send_frame_to_inference] response from INFERENCE is not OK: {await response.text()}")
                asyncio.sleep(1)
                continue
            return res

    return None


@app.post("/scenario/")
async def create_scenario():
    message_id = uuid.uuid1().hex
    scenario_id = uuid.uuid1().hex

    message = MyMessage( 
        message_id=message_id, 
        sender='api', 
        to='orchestrator', 
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
    # but it can be not executed
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
    session = Session_db()
    stmt = select(Scenario).filter(
        Scenario.scenario_id == scenario_id
    )

    result = session.execute(stmt)
    scenario = result.scalars().first()
    if scenario:
        return {"status": 200, "details": {"status": scenario.status, "scenario_id": scenario_id, "payload": scenario.payload}}
    

    orchestrator_answer = await ask_orchestrator_actual_status(scenario_id)
    if orchestrator_answer:
        return {"status": 200, "details": orchestrator_answer}
    
    return {"status": 404, "details": "Not found"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)