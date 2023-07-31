from enum import Enum


class ProgramStatusType1(str, Enum):
    PENDING = "Pending"

    def __str__(self) -> str:
        return str(self.value)
