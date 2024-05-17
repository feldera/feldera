from typing import Optional


class Program:
    """
    Represents a Feldera SQL program
    """

    name: Optional[str] = None
    code: Optional[str] = None
    description: Optional[str] = None
    id: Optional[str] = None
    version: Optional[int] = None
    status: Optional[str] = None

    def __init__(self, name: str, code: str, description: str = None, status: Optional[str] = None, version: Optional[int] = None, id: Optional[str] = None) -> None:
        self.name = name
        self.code = code
        self.description = description
        self.status = status
        self.version = version
        self.id = id

