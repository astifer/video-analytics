from fastapi import FastAPI, HTTPException

from pydantic import BaseModel
from enum import Enum
from uuid import uuid4
from typing import Dict

from shared.kafka_client import KafkaProducerWrapper, KafkaConsumerWrapper
from shared.scenario_models import Scenario, ScenarioUpdate, ScenarioCreate, ScenarioStatus, PredictionResult, is_transition_allowed

import logging
import aiohttp

import time

RUNNER_URL = "http://runner_service:7878"
logger = logging.getLogger(__name__)
app = FastAPI(title="Orchestrator Service")

scenarios: Dict[str, Scenario] = {}
predictions: Dict[str, PredictionResult] = {}


@app.post("/scenario/", response_model=Scenario)
async def create_scenario(scenario_data: ScenarioCreate):
    scenario_id = str(uuid4())
    scenario = Scenario(id=scenario_id, status=ScenarioStatus.init_startup)
    scenarios[scenario_id] = scenario
    
    return scenario

@app.post("/scenario/{scenario_id}/", response_model=Scenario)
async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail="Scenario not found")
    
    current_status = scenarios[scenario_id].status
    new_status = update.new_status

    if new_status == current_status:
        raise HTTPException(
            status_code=302,
            detail=f"Scenario already has this status"
        )
    
    if not is_transition_allowed(current_status, new_status):
        raise HTTPException(
            status_code=400,
            detail=f"Transition from {current_status} to {new_status} is not allowed."
        )
    
    scenarios[scenario_id].status = new_status

    if new_status == ScenarioStatus.active:

        async with aiohttp.ClientSession() as session:
            async with session.post(f"{RUNNER_URL}/process-stream/") as response:
                predictions = await response.json()

                scenarios[scenario_id].data = predictions
                return scenarios[scenario_id]

@app.get("/scenario/{scenario_id}/", response_model=Scenario)
async def get_scenario(scenario_id: str):
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return scenarios[scenario_id]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=1612, reload=True)