from enum import Enum


class KafkaLogLevel(str, Enum):
    ALERT = "alert"
    CRITICAL = "critical"
    DEBUG = "debug"
    EMERG = "emerg"
    ERROR = "error"
    INFO = "info"
    NOTICE = "notice"
    WARNING = "warning"

    def __str__(self) -> str:
        return str(self.value)
