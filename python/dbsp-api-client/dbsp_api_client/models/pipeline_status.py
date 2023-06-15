from enum import Enum


class PipelineStatus(str, Enum):
    DEPLOYED = "Deployed"
    FAILED = "Failed"
    PAUSED = "Paused"
    RUNNING = "Running"
    SHUTDOWN = "Shutdown"

    def __str__(self) -> str:
        return str(self.value)
