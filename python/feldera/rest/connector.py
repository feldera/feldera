from typing import Any, Mapping, Optional
from feldera.rest.attached_connector import AttachedConnector


class Connector:
    """
    A generic connector class that can be used to represent any Feldera connector
    """

    def __init__(
        self,
        name: str,
        description: Optional[str] = None,
        config: Optional[Mapping[str, Any]] = None,
        id: Optional[str] = None,
    ):
        """
        Create a new connector

        :param name: The name of the connector
        :param description: A description of the connector
        :param config: The configuration of the connector
        :param id: Optional. The ID of the connector. Not to be set by the user

        """

        self.name: str = name
        self.config: Mapping[str, Any] = config or {}
        self.description: Optional[str] = description
        self.id: Optional[str] = id

    def attach_relation(self, relation_name: str, is_input: bool) -> AttachedConnector:
        return AttachedConnector(self.name, relation_name, is_input)
