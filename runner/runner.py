import cv2
from PIL import Image
import io
from fastapi import FastAPI
from pydantic import BaseModel
import aiohttp
import asyncio
import datetime

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.status_models import ScenarioStatus
from shared.transactional_outbox import OutboxManager
from shared.database import Scenario

from shared.utils import settings
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    global session, outbox_manager, Session_db

    producer = KafkaProducerWrapper()
    orchestrator_consumer = KafkaConsumerWrapper(topic='orchestrator-to-runner', group_id="runner")
    outbox_manager = OutboxManager(db_url=settings.db_url, kafka_producer=producer)

    # Initialize HTTP session
    session = aiohttp.ClientSession()

    # Initialize database session
    engine = create_engine(settings.db_url)
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
    target = message_value.get("target")
    payload =  message_value.get("payload", {})
    scenario_id = payload.get("scenario_id")
    status = payload.get("status")

    if status == ScenarioStatus.IN_STARTUP_PROCESSING or target == "preprocess":
        await run_preprocess_scenario()
    elif status == ScenarioStatus.ACTIVE or target == "inference":
        await run_active_scenario(scenario_id)
    else:
        print(f"Runner got message with not suitable status: {status}. Runner want IN_STARTUP_PROCESSING or ACTIVE statuses. {scenario_id=}")

async def run_preprocess_scenario(scenario_id):
    frame = await frame_stream(STREAM_URL)
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

    stmt = select(Scenario).filter(
        Scenario.scenario_id == scenario_id
    )
    result = session_db.execute(stmt)
    scenario = result.scalars().first()
    scenario.status = ScenarioStatus.ACTIVE
    payload = scenario.payload
    frame = payload.get("frame")

    if not frame:
        print(f"Not found frame for this scenario. {scenario_id=}")
        return
    inference_result = await send_frame_to_inference(frame)
    scenario.payload['inference_result'] = inference_result
    scenario.processed_at = datetime.datetime.now(tz=settings.time_zone)
    await outbox_manager.save_message(
        message={
            "target": "inference_result",
            "payload": {
                "scenario_id": scenario_id,
                "inference_result": scenario.payload,
                "status": ScenarioStatus.ACTIVE,
                "processed_at": scenario.processed_at
            }
        },
        from_service="runner",
        target_service="orchestrator"
    )
    session_db.commit()
    session_db.close()

async def send_frame_to_inference(frame):
    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
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
    for _ in range(3):
        async with session.post(f"{INFERENCE_URL}/inference/", data=form) as response:
            res = await response.json()
            if response.status != 200:
                print(f"[send_frame_to_inference] response from INFERENCE is not OK: {await response.text()}")
                asyncio.sleep(2)
                continue

            return res


async def frame_stream(stream_url: str):
    # кажется тут упираемся в синхронность и ждем пока получим кадр
    cap = cv2.VideoCapture(stream_url)

    if not cap.isOpened():
        raise RuntimeError(f"Failed to open video stream: {stream_url}")

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                continue  # Retry
            return frame
    finally:
        cap.release()

def plot_boxes(frame, predictions):
    for el in predictions:
        label = el['label']
        x, y, x2, y2 = el['bb']

        cv2.rectangle(img=frame, pt1=(x, y), pt2=(x2, y2), color=(255, 0, 0), thickness=2)

        cv2.putText( 
                img=frame, 
                text=label, 
                org=(x, y + 20),
                fontFace=2,
                fontScale=0.5,
                color=(0,0,0),
                thickness=2
                )
        
    return frame


class AliveResponse(BaseModel):
    status_code: int

@app.get("/status/", response_model=AliveResponse)
async def get_status():
    resp = AliveResponse(status_code=200)
    return resp

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("runner:app", host="0.0.0.0", port=7878, reload=True)