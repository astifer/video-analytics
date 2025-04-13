from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
import aiohttp
import logging

logger = logging.getLogger(__name__)

app = FastAPI(title="Video Analytics API")

ORCHESTRATOR_URL = "http://orchestrator:1612"

class ScenarioStatus(str, Enum):
    init_startup = "init_startup"
    in_startup_processing = "in_startup_processing"
    active = "active"
    init_shutdown = "init_shutdown"
    in_shutdown_processing = "in_shutdown_processing"
    inactive = "inactive"

class ScenarioCreate(BaseModel):
    initial_status: ScenarioStatus

class ScenarioUpdate(BaseModel):
    new_status: ScenarioStatus

@app.post("/scenario/")
async def create_scenario(scenario_data: ScenarioCreate):
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{ORCHESTRATOR_URL}/scenario/", json=scenario_data.model_dump()) as response:
            res = await response.json()
            if response.status != 200:
                logger.error(f"[create_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
                raise HTTPException(status_code=response.status, detail=await response.text())
            return res

@app.post("/scenario/{scenario_id}/")
async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/", json=update.model_dump()) as response:
            res = await response.json()
            if response.status != 200:
                logger.error(f"[update_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
                raise HTTPException(status_code=response.status, detail=await response.text())
            return res

@app.get("/scenario/{scenario_id}/")
async def get_scenario(scenario_id: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/") as response:
            res = await response.json()
            if response.status != 200:
                logger.error(f"[get_scenario] response from ORCHESTRATOR is not OK: {await response.text()}")
                raise HTTPException(status_code=response.status, detail=await response.text())
            return res

@app.get("/prediction/{scenario_id}/")
async def get_prediction(scenario_id: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{ORCHESTRATOR_URL}/prediction/{scenario_id}/") as response:
            res = await response.json()
            if response.status != 200:
                logger.error(f"[get_prediction] response from ORCHESTRATOR is not OK: {await response.text()}")
                raise HTTPException(status_code=response.status, detail=await response.text())
            return res

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)