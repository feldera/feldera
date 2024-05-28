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
        """
        Initializes a new pipeline

        :param name: The name of the pipeline
        :param program_name: The name of the program that the pipeline is based on
        :param description: Optional. The description of the pipeline
        :param attached_connectors: Optional. The connectors attached to the pipeline
        :param config: Optional. The configuration of the pipeline
        :param state: Optional. The state of the pipeline. Not to be set by the user
        :param version: The version of the pipeline. Not to be set by the user
        :param id: Optional. The id of the pipeline. Not to be set by the user

        """

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

    def current_state(self) -> Optional[str]:
        """
        Returns the current state of this pipeline
        """

        return self.state.get("current_status")
