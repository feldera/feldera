from enum import Enum


class ProgramStatusType3(str, Enum):
    COMPILINGRUST = "CompilingRust"

    def __str__(self) -> str:
        return str(self.value)
