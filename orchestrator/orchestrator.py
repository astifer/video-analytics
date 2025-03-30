from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from enum import Enum
from uuid import uuid4
from typing import Dict
from models import Scenario, ScenarioCreate, ScenarioStatus, ScenarioUpdate, PredictionResult, is_transition_allowed

app = FastAPI(title="Orchestrator Service")

scenarios: Dict[str, Scenario] = {}
predictions: Dict[str, PredictionResult] = {}

@app.post("/scenario/", response_model=Scenario)
async def create_scenario(scenario_data: ScenarioCreate):
    scenario_id = str(uuid4())
    scenario = Scenario(id=scenario_id, status=scenario_data.initial_status)
    scenarios[scenario_id] = scenario
    return scenario

@app.post("/scenario/{scenario_id}/", response_model=Scenario)
async def update_scenario(scenario_id: str, update: ScenarioUpdate):
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail="Scenario not found")
    
    current_status = scenarios[scenario_id].status
    new_status = update.new_status

    if not is_transition_allowed(current_status, new_status):
        raise HTTPException(
            status_code=400,
            detail=f"Transition from {current_status} to {new_status} is not allowed."
        )
    
    scenarios[scenario_id].status = new_status
    return scenarios[scenario_id]

@app.get("/scenario/{scenario_id}/", response_model=Scenario)
async def get_scenario(scenario_id: str):
    if scenario_id not in scenarios:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return scenarios[scenario_id]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=1612, reload=True)