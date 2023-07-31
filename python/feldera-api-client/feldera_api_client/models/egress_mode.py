from enum import Enum


class EgressMode(str, Enum):
    SNAPSHOT = "snapshot"
    WATCH = "watch"

    def __str__(self) -> str:
        return str(self.value)
