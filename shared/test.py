import  asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager

async def consume():
    while True:
        print('Look for peding..')
        await asyncio.sleep(2)

async def other_func():
    while True:
        print("Send some...")
        await asyncio.sleep(3)

@asynccontextmanager
async def lifespan(app: FastAPI):

    asyncio.create_task(consume())
    asyncio.create_task(other_func())

    # await asyncio.gather(task1, task2)
    yield
    print("Ending...")
    await asyncio.sleep(2)

app = FastAPI(title="Video Analytics API", lifespan=lifespan)
