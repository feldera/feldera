from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.attached_connector import AttachedConnector
    from ..models.runtime_config import RuntimeConfig


T = TypeVar("T", bound="UpdatePipelineRequest")


@define
class UpdatePipelineRequest:
    """Request to update an existing pipeline.

    Attributes:
        description (str): New pipeline description.
        name (str): New pipeline name.
        config (Union[Unset, None, RuntimeConfig]): Global pipeline configuration settings. This is the publicly
            exposed type for users to configure pipelines.
        connectors (Union[Unset, None, List['AttachedConnector']]): Attached connectors.

            - If absent, existing connectors will be kept unmodified.

            - If present all existing connectors will be replaced with the new
            specified list.
        program_name (Union[Unset, None, str]): New program to create a pipeline for. If absent, program will be set to
            NULL.
    """

    description: str
    name: str
    config: Union[Unset, None, "RuntimeConfig"] = UNSET
    connectors: Union[Unset, None, List["AttachedConnector"]] = UNSET
    program_name: Union[Unset, None, str] = UNSET

    def to_dict(self) -> Dict[str, Any]:
        description = self.description
        name = self.name
        config: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict() if self.config else None

        connectors: Union[Unset, None, List[Dict[str, Any]]] = UNSET
        if not isinstance(self.connectors, Unset):
            if self.connectors is None:
                connectors = None
            else:
                connectors = []
                for connectors_item_data in self.connectors:
                    connectors_item = connectors_item_data.to_dict()

                    connectors.append(connectors_item)

        program_name = self.program_name

        field_dict: Dict[str, Any] = {}
        field_dict.update(
            {
                "description": description,
                "name": name,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config
        if connectors is not UNSET:
            field_dict["connectors"] = connectors
        if program_name is not UNSET:
            field_dict["program_name"] = program_name

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.attached_connector import AttachedConnector
        from ..models.runtime_config import RuntimeConfig

        d = src_dict.copy()
        description = d.pop("description")

        name = d.pop("name")

        _config = d.pop("config", UNSET)
        config: Union[Unset, None, RuntimeConfig]
        if _config is None:
            config = None
        elif isinstance(_config, Unset):
            config = UNSET
        else:
            config = RuntimeConfig.from_dict(_config)

        connectors = []
        _connectors = d.pop("connectors", UNSET)
        for connectors_item_data in _connectors or []:
            connectors_item = AttachedConnector.from_dict(connectors_item_data)

            connectors.append(connectors_item)

        program_name = d.pop("program_name", UNSET)

        update_pipeline_request = cls(
            description=description,
            name=name,
            config=config,
            connectors=connectors,
            program_name=program_name,
        )

        return update_pipeline_request
