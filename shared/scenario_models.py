from pydantic import BaseModel
from typing import Dict, Optional, Any, Literal
from .status_models import ScenarioStatus


class Scenario(BaseModel):
    id: str
    status: ScenarioStatus
    data: dict = {}

class ScenarioCreate(BaseModel):
    initial_status: ScenarioStatus

class ScenarioUpdate(BaseModel):
    new_status: ScenarioStatus

class PredictionResult(BaseModel):
    scenario_id: str
    predictions: Optional[Dict] = {}

class MyMessage(BaseModel):
    message_id: str
    payload: dict = {}
    sender: Literal["api", "orchestrator", "runner", "inference"]
    to: Literal["api", "orchestrator", "runner", "inference"]