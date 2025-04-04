import cv2
import requests
from PIL import Image
import io
import numpy as np
import time
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pydantic
from pydantic import BaseModel
import httpx
from typing import Any

app = FastAPI(title="Runner Service")

class AliveResponse(BaseModel):
    status_code: int

class StreamResponse(BaseModel):
    content: Any

STREAM_URL = "https://s35.ipcamlive.com/streams/23fmhujpncmqvpew3/stream.m3u8"

async def send_frame_to_inference(frame):
    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    pil_image = Image.fromarray(rgb_frame)

    buf = io.BytesIO()
    pil_image.save(buf, format="JPEG")
    buf.seek(0)

    files = {'file': ("frame.jpg", buf, "image/jpeg")}

    async with httpx.AsyncClient() as client:
        response = await client.post("http://inference_service:8001/inference/", files=files)
    
    if response.status_code == 200:
        return response.json()
    return None

def frame_stream(stream_url: str):
    cap = cv2.VideoCapture(stream_url)

    if not cap.isOpened():
        raise RuntimeError(f"Failed to open video stream: {stream_url}")

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                continue  # Stream hiccup? Retry.
            yield frame
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
async def process_stream():
    frame_gen = frame_stream(STREAM_URL)

    for frame in frame_gen:
        result = await send_frame_to_inference(frame)

        if result is not None:
            return JSONResponse(content=result)

    return {"message": "No valid frame was processed"}


@app.get("/status/", response_model=AliveResponse)
async def get_status():
    resp = AliveResponse(status_code=200)
    return resp

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("runner:app", host="0.0.0.0", port=7878, reload=True)