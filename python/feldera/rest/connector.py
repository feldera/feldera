from typing import Any, Mapping, Optional
from feldera.rest.attached_connector import AttachedConnector


class Connector:
    """
    A generic connector class that can be used to represent any Feldera connector
    """
    name: str
    description: Optional[str]
    config: Mapping[str, Any]
    id: Optional[str]

    def __init__(
        self,
        name: str,
        id: Optional[str] = None,
        config: Optional[Mapping[str, Any]] = None,
        description: Optional[str] = None
    ):
        self.name = name
        self.config = config or {}
        self.description = description
        self.id = id

    def attach_relation(self, relation_name: str, is_input: bool) -> AttachedConnector:
        return AttachedConnector(self.name, relation_name, is_input)
