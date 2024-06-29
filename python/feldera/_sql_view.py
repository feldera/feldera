from enum import Enum
from typing import List

class ViewKind(Enum):
    DEFAULT = 1
    LOCAL = 2
    MATERIALIZED = 3


class SQLView:
    def __init__(self, name: str, kind: ViewKind, query: str):
        self.name: str = name
        self.kind = kind

        query = query.strip()
        if query[-1] != ';':
            query += ';'

        self.query: str = query
        self.lateness: List[str] = []

    def add_lateness(self, timestamp_column: str, lateness_expr: str):
        self.lateness.append(f"LATENESS {self.name}.{timestamp_column} {lateness_expr};")

    def build_ddl(self):
        match self.kind:
            case ViewKind.DEFAULT:
                kind = ""
            case ViewKind.LOCAL:
                kind = " LOCAL"
            case ViewKind.MATERIALIZED:
                kind = " MATERIALIZED"
        view = f"CREATE{kind} VIEW {self.name} AS {self.query}"
        statements = self.lateness + [view]
        return "\n".join(statements)