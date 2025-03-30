from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from uuid import uuid4
from enum import Enum
from typing import Dict, Optional

app = FastAPI(title="Video Analytics Api")


class ScenarioStatus(str, Enum):
    init_startup = "init_startup"
    in_startup_processing = "in_startup_processing"
    active = "active"
    init_shutdown = "init_shutdown"
    in_shutdown_processing = "in_shutdown_processing"
    inactive = "inactive"


ALLOWED_TRANSITIONS = {
    ScenarioStatus.init_startup: [ScenarioStatus.in_startup_processing],
    ScenarioStatus.in_startup_processing: [ScenarioStatus.active],
    ScenarioStatus.init_shutdown: [ScenarioStatus.in_shutdown_processing],
    ScenarioStatus.in_shutdown_processing: [ScenarioStatus.inactive],
}


class ScenarioCreate(BaseModel):
    initial_status: ScenarioStatus = ScenarioStatus.init_startup

class ScenarioUpdate(BaseModel):
    new_status: ScenarioStatus

class Scenario(BaseModel):
    id: str
    status: ScenarioStatus

class PredictionResult(BaseModel):
    scenario_id: str
    predictions: Optional[Dict] = {}


scenarios: Dict[str, Scenario] = {}
predictions: Dict[str, PredictionResult] = {}

def is_transition_allowed(current_status: ScenarioStatus, new_status: ScenarioStatus) -> bool:
    """
    Проверяет, является ли переход от current_status к new_status допустимым.
    """
    allowed = ALLOWED_TRANSITIONS.get(current_status, [])
    return new_status in allowed

@app.post("/scenario/", response_model=Scenario)
async def create_scenario(scenario_data: ScenarioCreate):
    scenario_id = str(uuid4())
    scenario = Scenario(id=scenario_id, status=scenario_data.initial_status)
    scenarios[scenario_id] = scenario

    predictions[scenario_id] = PredictionResult(scenario_id=scenario_id, predictions={})
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

@app.get("/prediction/{scenario_id}/", response_model=PredictionResult)
async def get_prediction(scenario_id: str):
    if scenario_id not in predictions:
        raise HTTPException(status_code=404, detail="Prediction not found")
    return predictions[scenario_id]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
