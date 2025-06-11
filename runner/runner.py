import cv2
from PIL import Image
import io
import numpy as np
import time
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pydantic
from pydantic import BaseModel
import aiohttp
from typing import Any
import logging
import json
import asyncio

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.utils import get_urls, Settings
from contextlib import asynccontextmanager


producer = KafkaProducerWrapper()
consumer = KafkaConsumerWrapper(topic='orchestrator-to-runner', group_id="runner")

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

    await producer.stop()
    await consumer.stop()
    await session.close()


public_urls = get_urls()
STREAM_URL = public_urls.get("STREAM_URL")
INFERENCE_URL = public_urls.get("INFERENCE_URL")


logger = logging.getLogger(__name__)
app = FastAPI(title="Runner Service", lifespan=lifespan)


# dict for start and shutdown instances
class AliveResponse(BaseModel):
    status_code: int

class StreamResponse(BaseModel):
    content: Any


async def consume_messages():
    """Consume messages from Kafka and process them."""
    while True:
        try:
            break
            message = await consumer.get_message()
            if message:
                await process_message(message)
        except Exception as e:
            logger.error(f"Error consuming message: {str(e)}")
        await asyncio.sleep(0.1)

async def process_message(message):
    """Process a message from the outbox."""
    return
    try:
        data = json.loads(message.value)
        message_type = data.get('message_type')
        payload = data.get('payload')
        
        if message_type == 'runner_process_stream':
            # Process the stream as requested
            scenario_id = payload.get('scenario_id')
            if scenario_id:
                result = await process_stream()
                # Send result back to orchestrator
                await producer.send_message(
                    topic='runner-to-orch-results',
                    value=json.dumps({
                        'scenario_id': scenario_id,
                        'result': result
                    })
                )
        else:
            logger.warning(f"Unknown message type: {message_type}")
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")

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

    async with session.post(f"{INFERENCE_URL}/inference/", data=form) as response:
        res = await response.json()
        if response.status != 200:
            logger.error(f"[send_frame_to_inference] response from INFERENCE is not OK: {await response.text()}")
            return False
        
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
    
@app.post("/process-stream/", response_model=StreamResponse)
async def process_stream(data: Any = None):
    frame = await frame_stream(STREAM_URL)
    result = await send_frame_to_inference(frame)

    if result:
        return JSONResponse(content=result)

    return {"message": "No valid frame was processed"}


@app.get("/status/", response_model=AliveResponse)
async def get_status():
    resp = AliveResponse(status_code=200)
    return resp

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("runner:app", host="0.0.0.0", port=7878, reload=True)