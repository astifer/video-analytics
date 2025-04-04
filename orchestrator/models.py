from pydantic import BaseModel
from typing import Dict, Optional, Any
from enum import Enum

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
    ScenarioStatus.active: [ScenarioStatus.init_shutdown],
    ScenarioStatus.init_shutdown: [ScenarioStatus.in_shutdown_processing],
    ScenarioStatus.in_shutdown_processing: [ScenarioStatus.inactive]
}

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

def is_transition_allowed(current_status: ScenarioStatus, new_status: ScenarioStatus) -> bool:
    return new_status in ALLOWED_TRANSITIONS.get(current_status, [])
