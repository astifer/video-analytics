from pydantic import BaseModel
from typing import Dict, Optional, Any
# from shared.status_models import ScenarioStatus

class Scenario(BaseModel):
    id: str
    status: str = 'ScenarioStatus'
    data: dict = {}

scenario = Scenario(id='s')

import uuid

s = uuid.uuid1()
print(type(s.hex))