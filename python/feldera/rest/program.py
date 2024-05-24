from typing import Optional


class Program:
    """
    Represents a Feldera SQL program
    """

    def __init__(self, name: str, code: str, description: str = None, status: Optional[str] = None, version: Optional[int] = None, id: Optional[str] = None) -> None:
        """
        Initializes a new instance of the Program class

        :param name: The name of the program
        :param code: The SQL code (DDL) for this program
        :param description: Optional. Description of the program
        :param status: Optional. The status of the program. Not necessary for creation.
        :param version: Optional. The version of the program. Not necessary for creation.
        :param id: Optional. The id of the program. Not necessary for creation.

        """

        self.name: Optional[str] = name
        self.code: Optional[str] = code
        self.description: Optional[str] = description
        self.status: Optional[str] = status
        self.version: Optional[int] = version
        self.id: Optional[str] = id
