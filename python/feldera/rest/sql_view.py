class SQLView:
    """
    Represents a SQL view in Feldera
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
    def from_dict(self, view_dict: dict):
        tbl = SQLView(name=view_dict["name"], fields=view_dict["fields"])
        tbl.case_sensitive = view_dict["case_sensitive"]
        tbl.materialized = view_dict["materialized"]
        return tbl
