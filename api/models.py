from typing import Any
from pydantic import BaseModel
from shared.status_models import ScenarioStatus

class Scenario(BaseModel):
    scenario_id: str
    status: ScenarioStatus
    payload: dict = {}
    prediction: dict = {}