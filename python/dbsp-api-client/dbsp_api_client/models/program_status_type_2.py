from enum import Enum


class ProgramStatusType2(str, Enum):
    COMPILINGSQL = "CompilingSql"

    def __str__(self) -> str:
        return str(self.value)
