from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
import requests
import os

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
    response = requests.post(f"{ORCHESTRATOR_URL}/scenario/", json=scenario_data.dict())
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()

@app.post("/scenario/{scenario_id}/")
async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    response = requests.post(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/", json=update.dict())
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()

@app.get("/scenario/{scenario_id}/")
async def get_scenario(scenario_id: str):
    response = requests.get(f"{ORCHESTRATOR_URL}/scenario/{scenario_id}/")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()

@app.get("/prediction/{scenario_id}/")
async def get_prediction(scenario_id: str):
    response = requests.get(f"{ORCHESTRATOR_URL}/prediction/{scenario_id}/")
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
