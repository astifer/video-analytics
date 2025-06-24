import cv2
from PIL import Image
import io
from fastapi import FastAPI
from pydantic import BaseModel
import aiohttp
import asyncio
import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.status_models import ScenarioStatus
from shared.transactional_outbox import OutboxManager
from shared.database import Scenario, find_scenario

from shared.utils import settings, make_async_post_request_with_retry
from shared.scenario_models import MyMessage

from contextlib import asynccontextmanager


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
