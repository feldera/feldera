from typing import Optional
from typing_extensions import Self
from enum import Enum


class UpdateFormat(Enum):
    """
    Enum for the update format of the JSON format.

    https://www.feldera.com/docs/api/json/#the-insertdelete-format
    """

    InsertDelete = 1
    """
    This is a way to represented changes in the data.
    When used, the actual JSON data is expected to be inside one of the keys "insert" or "delete".
    Data inside "insert" is considered to be an insert operation, and data inside "delete" is considered to be a
    delete operation.
    
    Example: `{"insert": {"id": 1, "name": "Alice"}, "delete": {"id": 2, "name": "Bob"}}`
    """

    Raw = 2
    """
    This format represents an individual row in a SQL table or view.
    
    Example: `{"id": 1, "name": "Alice"}`
    """

    def __str__(self):
        match self:
            case UpdateFormat.InsertDelete:
                return "insert_delete"
            case UpdateFormat.Raw:
                return "raw"


class JSONFormat:
    """
    Used to represent data ingested and output from Feldera in the JSON format.
    """

    def __init__(self, config: Optional[dict] = None):
        self.config: dict = config or {
            "array": False,
        }

    def with_update_format(self, update_format: UpdateFormat) -> Self:
        self.config["update_format"] = update_format.__str__()
        return self

    def with_array(self, array: bool) -> Self:
        self.config["array"] = array
        return self

    def to_dict(self):
        """
        Serialize to a dict to be used in the API request.

        :meta private:
        """
        return {
            "name": "json",
            "config": self.config
        }


class CSVFormat:
    """
    Used to represent data ingested and output from Feldera in the CSV format.
    
    https://www.feldera.com/docs/api/csv
    """

    def to_dict(self) -> dict:
        """
        Serialize to a dict to be used in the API request.

        :meta private:
        """
        return {
            "name": "csv"
        }
