from typing import Optional
from typing_extensions import Self
from enum import Enum


class JSONUpdateFormat(Enum):
    """
    Supported JSON data change event formats.

    Each element in a JSON-formatted input stream specifies
    an update to one or more records in an input table.  We support
    several different ways to represent such updates.

    https://www.feldera.com/docs/api/json/#the-insertdelete-format
    """

    InsertDelete = 1
    """
    Insert/delete format.
    
    Each element in the input stream consists of an "insert" or "delete"
    command and a record to be inserted to or deleted from the input table.
    
    Example: `{"insert": {"id": 1, "name": "Alice"}, "delete": {"id": 2, "name": "Bob"}}`
    Here, `id` and `name` are the columns in the table.
    """

    Raw = 2
    """
    Raw input format.
    
    This format is suitable for insert-only streams (no deletions).
    Each element in the input stream contains a record without any
    additional envelope that gets inserted in the input table.
    
    Example: `{"id": 1, "name": "Alice"}`
    Here, `id` and `name` are the columns in the table.
    """

    def __str__(self):
        match self:
            case JSONUpdateFormat.InsertDelete:
                return "insert_delete"
            case JSONUpdateFormat.Raw:
                return "raw"


class JSONFormat:
    """
    Used to represent data ingested and output from Feldera in the JSON format.
    """

    def __init__(self, config: Optional[dict] = None):
        """
        Creates a new JSONFormat instance.

        :param config: Optional. Configuration for the JSON format.
        """

        self.config: dict = config or {
            "array": False,
        }

    def with_update_format(self, update_format: JSONUpdateFormat) -> Self:
        """
        Specifies the format of the data change events in the JSON data stream.
        """

        self.config["update_format"] = update_format.__str__()
        return self

    def with_array(self, array: bool) -> Self:
        """
        Set to `True` if updates in this stream are packaged into JSON arrays.

        Example: `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
        """

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
