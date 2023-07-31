from enum import Enum


class ProgramStatusType4(str, Enum):
    SUCCESS = "Success"

    def __str__(self) -> str:
        return str(self.value)
