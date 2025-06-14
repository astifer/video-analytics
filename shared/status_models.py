from enum import Enum

class MessageStatus(str, Enum):
    PENDING = 'pending'
    PROCESSING = 'processing'
    PROCESSED = 'processed'
    FAILED = 'failed'

class ScenarioStatus(str, Enum):
    INIT_STARTUP = 'init_startup'
    IN_STARTUP_PROCESSING = 'in_startup_processing'
    ACTIVE = 'active'
    INIT_SHUTDOWN = 'init_shutdown'
    IN_SHUTDOWN_PROCESSING = 'in_shutdown_processing'
    INACTIVE = 'inactive'

# Define allowed transitions for scenario statuses
ALLOWED_TRANSITIONS = {
    ScenarioStatus.INIT_STARTUP: [ScenarioStatus.IN_STARTUP_PROCESSING],
    ScenarioStatus.IN_STARTUP_PROCESSING: [ScenarioStatus.ACTIVE],
    ScenarioStatus.ACTIVE: [ScenarioStatus.INIT_SHUTDOWN],
    ScenarioStatus.INIT_SHUTDOWN: [ScenarioStatus.IN_SHUTDOWN_PROCESSING],
    ScenarioStatus.IN_SHUTDOWN_PROCESSING: [ScenarioStatus.INACTIVE]
}

def is_transition_allowed(current_status: ScenarioStatus, new_status: ScenarioStatus) -> bool:
    return new_status in ALLOWED_TRANSITIONS.get(current_status, []) 