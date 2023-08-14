from enum import Enum


class JsonUpdateFormat(str, Enum):
    DEBEZIUM = "debezium"
    INSERT_DELETE = "insert_delete"
    RAW = "raw"
    WEIGHTED = "weighted"

    def __str__(self) -> str:
        return str(self.value)
