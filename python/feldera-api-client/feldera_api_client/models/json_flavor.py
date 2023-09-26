from enum import Enum


class JsonFlavor(str, Enum):
    DEBEZIUM_MYSQL = "debezium_mysql"
    DEFAULT = "Default"

    def __str__(self) -> str:
        return str(self.value)
