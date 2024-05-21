from typing import Optional


class Program:
    """
    Represents a Feldera SQL program
    """

    def __init__(self, name: str, code: str, description: str = None, status: Optional[str] = None, version: Optional[int] = None, id: Optional[str] = None) -> None:
        self.name: Optional[str] = name
        self.code: Optional[str] = code
        self.description: Optional[str] = description
        self.status: Optional[str] = status
        self.version: Optional[int] = version
        self.id: Optional[str] = id

