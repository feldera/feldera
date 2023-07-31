from enum import Enum


class OutputQuery(str, Enum):
    NEIGHBORHOOD = "neighborhood"
    QUANTILES = "quantiles"
    TABLE = "table"

    def __str__(self) -> str:
        return str(self.value)
