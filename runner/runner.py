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

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper

app = FastAPI(title="Runner Service")
logger = logging.getLogger(__name__)

# dict for start and shutdosn isntances
# topics in kafka
# aiokafka new versions for non zookeeper


class AliveResponse(BaseModel):
    status_code: int

class StreamResponse(BaseModel):
    content: Any

STREAM_URL = "https://s46.ipcamlive.com/streams/2eulqgccb8zksexmj/stream.m3u8"
INFERENCE_URl = "http://inference_service:8001"

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

    async with aiohttp.ClientSession() as session:
        async with session.post(f"{INFERENCE_URl}/inference/", data=form) as response:
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