import cv2
from PIL import Image
import io
from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np
import aiohttp
import asyncio
import datetime

import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.status_models import ScenarioStatus
from shared.transactional_outbox import OutboxManager
from shared.database import Scenario, find_scenario

from shared.utils import settings, make_async_post_request_with_retry
from contextlib import asynccontextmanager

from service import plot_boxes, frame_stream


@asynccontextmanager
async def lifespan(app: FastAPI):
    global session, outbox_manager, Session_db

    producer = KafkaProducerWrapper()
    orchestrator_consumer = KafkaConsumerWrapper(topic='orchestrator-to-runner', group_id="runner")
    outbox_manager = OutboxManager(db_url=settings.db_url, kafka_producer=producer)

    # Initialize HTTP session
    session = aiohttp.ClientSession()

    # Initialize database session
    engine = create_engine(settings.db_url, pool_size=20, max_overflow=0)
    Session_db = sessionmaker(bind=engine)

    # Initialize Kafka components
    await producer.start()
    await orchestrator_consumer.start()

    # Start Kafka consumer loop
    asyncio.create_task(outbox_manager.start_processing_loop(interval_seconds=1))
    asyncio.create_task(orchestrator_consumer.consume(process_messages_from_orchestrator))

    yield

    await session.close()
    await producer.stop()
    await orchestrator_consumer.stop()


public_urls = settings.public_urls
STREAM_URL = public_urls.get("STREAM_URL")
INFERENCE_URL = public_urls.get("INFERENCE_URL")


app = FastAPI(title="Runner Service", lifespan=lifespan)


async def process_messages_from_orchestrator(message_value: dict):
    """
    Receined message from orchestrator: {"message_id": "2276172c487111f080316619d9cb34fb", "payload": {"scenario_id": "22761a74487111f080316619d9cb34fb", "target": "init_scenario"}, "created_at": "2025-06-13T16:09:27.366037", "processed_at": "2025-06-13T19:12:06.336889+03:00"}
    """
    print(f"Received message from orchestrator: {message_value}")
    if isinstance(message_value, str):
        try:
            message_value = json.loads(message_value)
        except:
            print(f"Error while parsiing message from orchestrator {message_value=}")

    target = message_value.get("target")
    payload =  message_value.get("payload", {})
    scenario_id = payload.get("scenario_id")
    status = payload.get("status")

    if status == ScenarioStatus.IN_STARTUP_PROCESSING or target == "preprocess":
        await run_preprocess_scenario(scenario_id)
    elif status == ScenarioStatus.ACTIVE or target == "inference":
        await run_active_scenario(scenario_id)
    else:
        print(f"Runner got message with not suitable status: {status}. Runner want IN_STARTUP_PROCESSING or ACTIVE statuses. {scenario_id=}")

    print("Succesfully process mesages from orchestrator")


async def run_preprocess_scenario(scenario_id):
    frame = await frame_stream(stream_url=settings.public_urls.get("STREAM_URL"))
    session_db = Session_db()
    scenario = Scenario(
        scenario_id=scenario_id,
        payload={"frame": frame},
        status=ScenarioStatus.IN_STARTUP_PROCESSING
    )
    session_db.add(scenario)
    session_db.commit()
    session_db.close()


async def run_active_scenario(scenario_id):
    session_db = Session_db()

    scenario = find_scenario(session_db, scenario_id, close_session=False)
    if not scenario:
        return
    
    scenario.status = ScenarioStatus.ACTIVE
    payload = scenario.payload
    frame = payload.get("frame")

    if not frame:
        print(f"Not found frame for this scenario. {scenario_id=}")
        return
    if isinstance(frame, list):
        frame = np.array(frame,  dtype=np.uint8)

    inference_result = await send_frame_to_inference(frame)
    # end

    scenario.payload['inference_result'] = inference_result
    processed_at = datetime.datetime.now(tz=settings.time_zone).isoformat()
    scenario.processed_at = processed_at
    session_db.commit()
    session_db.close()

    await outbox_manager.save_message(
        message={
            "target": "inference_result",
            "payload": {
                "scenario_id": scenario_id,
                "target": "inference_result",
                "inference_result": inference_result,
                "status": ScenarioStatus.ACTIVE,
                "processed_at": processed_at
            }
        },
        from_service="runner",
        target_service="orchestrator"
    )



async def send_frame_to_inference(frame: np.ndarray):
    try:
        print(frame.shape)
        print(frame)
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    except Exception as e:
        print(f"cannot cast frame into cv2 object {type(frame)}, {frame.shape}, {e}")
        return {}
    
    print("collecting file to send to inference...")
    pil_image = Image.fromarray(rgb_frame)

    buf = io.BytesIO()
    pil_image.save(buf, format="JPEG")
    buf.seek(0)

    form = aiohttp.FormData()
    form.add_field(
        name='file',
        value=buf,
        filename='frame.jpg',
        content_type='image/jpeg'
    )
    form = aiohttp.FormData()
    form.add_field(
        name='file',
        value=buf,
        filename='frame.jpg',
        content_type='image/jpeg'
    )
    INFERENCE_URL = settings.public_urls.get("INFERENCE_URL")

    print("Collected! Sending to inference...")
    for _ in range(3):
        async with session.post( url=f"{INFERENCE_URL}/inference/", data=form) as response:
            res = await response.json()
            print(f"Got response {res}")
            if response.status != 200:
                print(f"[send_frame_to_inference] response from INFERENCE is not OK: {await response.text()}")
                await asyncio.sleep(2)
                continue

            return res
    



class AliveResponse(BaseModel):
    status_code: int

@app.get("/status/", response_model=AliveResponse)
async def get_status():
    resp = AliveResponse(status_code=200)
    return resp
