from enum import Enum


class PipelineStatus(str, Enum):
    FAILED = "Failed"
    INITIALIZING = "Initializing"
    PAUSED = "Paused"
    PROVISIONING = "Provisioning"
    RUNNING = "Running"
    SHUTDOWN = "Shutdown"
    SHUTTINGDOWN = "ShuttingDown"

    def __str__(self) -> str:
        return str(self.value)
