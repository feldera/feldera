from typing import Mapping, Any, Optional
from feldera.attached_connector import AttachedConnector


class Pipeline:
    """
    Represents a Feldera pipeline
    """
    id: Optional[str]
    name: str
    program_name: str
    description: Optional[str]
    attached_connectors: list[AttachedConnector]
    config: Mapping[str, Any]
    state: Optional[Mapping[str, Any]]
    version: int

    def __init__(
        self,
        name: str,
        program_name: str,
        description: Optional[str] = None,
        attached_connectors: Optional[list[AttachedConnector]] = None,
        config: Optional[Mapping[str, Any]] = None,
        state: Optional[Mapping[str, Any]] = None,
        version: int = 0,
        id: Optional[str] = None
    ):
        self.name = name
        self.program_name = program_name
        self.description = description
        self.attached_connectors = attached_connectors or []
        self.config = config or Pipeline.default_config()
        self.state = state
        self.version = version
        self.id = id

    @staticmethod
    def default_config() -> Mapping[str, Any]:
        """
        Returns the default configuration for the pipeline
        """
        # default, taken from the UI
        return {
            "workers": 8
        }
