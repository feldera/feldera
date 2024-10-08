class SQLTable:
    """
    Represents a SQL table in Feldera
    """

    def __init__(
        self,
        name: str,
        fields: list[dict],
        case_sensitive: bool = False,
        materialized: bool = False,
    ):
        self.name = name
        self.case_sensitive = case_sensitive
        self.materialized = materialized
        self.fields: list[dict] = fields

    @classmethod
    def from_dict(self, table_dict: dict):
        tbl = SQLTable(name=table_dict["name"], fields=table_dict["fields"])
        tbl.case_sensitive = table_dict["case_sensitive"]
        tbl.materialized = table_dict["materialized"]
        return tbl
