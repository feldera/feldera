from enum import Enum


class ProgramStatusType0(str, Enum):
    NONE = "None"

    def __str__(self) -> str:
        return str(self.value)
