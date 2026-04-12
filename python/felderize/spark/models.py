from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Status(str, Enum):
    SUCCESS = "success"
    UNSUPPORTED = "unsupported"
    ERROR = "error"


@dataclass
class TranslationResult:
    feldera_schema: str = ""
    feldera_query: str = ""
    unsupported: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    explanations: list[str] = field(default_factory=list)
    status: Status = Status.SUCCESS

    def to_dict(self) -> dict:
        return {
            "feldera_schema": self.feldera_schema,
            "feldera_query": self.feldera_query,
            "unsupported": self.unsupported,
            "warnings": self.warnings,
            "explanations": self.explanations,
            "status": self.status.value,
        }
