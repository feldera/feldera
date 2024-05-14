from typing import Optional
from feldera.sql_schema import SQLSchema


class SQLTable:
    name: str
    ddl: str
    schema: Optional[SQLSchema]

    def __init__(self, name: str, ddl: Optional[str] = None, schema: Optional[SQLSchema] = None):
        if ddl is None and schema is None:
            raise ValueError("Either ddl or schema must be provided")

        self.name = name
        self.ddl = ddl
        self.schema = schema

    def build_ddl(self):
        """
        Either returns the provided ddl or builds it from the schema
        """

        if self.schema is None:
            return self.ddl

        if self.ddl is not None:
            raise ValueError("Both ddl and schema are provided")

        return self.schema.build_ddl(self.name)