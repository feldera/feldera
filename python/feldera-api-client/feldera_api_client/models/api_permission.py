from enum import Enum


class ApiPermission(str, Enum):
    READ = "Read"
    WRITE = "Write"

    def __str__(self) -> str:
        return str(self.value)
