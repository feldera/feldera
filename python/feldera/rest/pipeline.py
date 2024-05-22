from typing import Mapping, Any, Optional
from feldera.rest.attached_connector import AttachedConnector


class Pipeline:
    """
    Represents a Feldera pipeline
    """

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
        self.name: str = name
        self.program_name: str = program_name
        self.description: Optional[str] = description
        self.attached_connectors: list[AttachedConnector] = attached_connectors or []
        self.config: Mapping[str, Any] = config or Pipeline.default_config()
        self.state: Optional[Mapping[str, Any]] = state
        self.version: int = version
        self.id: Optional[str] = id

    @staticmethod
    def default_config() -> Mapping[str, Any]:
        """
        Returns the default configuration for the pipeline
        """
        # default, taken from the UI
        return {
            "workers": 8
        }
