from typing import List

class SQLView:
    def __init__(self, name: str, local: bool, query: str):
        self.name: str = name
        self.local = local

        query = query.strip()
        if query[-1] != ';':
            query += ';'

        self.query: str = query
        self.lateness: List[str] = []

    def add_lateness(self, timestamp_column: str, lateness_expr: str):
        self.lateness.append(f"LATENESS {self.name}.{timestamp_column} {lateness_expr};")

    def build_ddl(self):
        local = " LOCAL" if self.local else ""
        view = f"CREATE{local} VIEW {self.name} AS {self.query}"
        statements = self.lateness + [view]
        return "\n".join(statements)