from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.attached_connector import AttachedConnector
    from ..models.runtime_config import RuntimeConfig


T = TypeVar("T", bound="NewPipelineRequest")


@define
class NewPipelineRequest:
    """Request to create a new pipeline.

    Attributes:
        config (RuntimeConfig): Global pipeline configuration settings. This is the publicly
            exposed type for users to configure pipelines.
        description (str): Pipeline description.
        name (str): Unique pipeline name.
        connectors (Union[Unset, None, List['AttachedConnector']]): Attached connectors.
        program_name (Union[Unset, None, str]): Name of the program to create a pipeline for.
    """

    config: "RuntimeConfig"
    description: str
    name: str
    connectors: Union[Unset, None, List["AttachedConnector"]] = UNSET
    program_name: Union[Unset, None, str] = UNSET

    def to_dict(self) -> Dict[str, Any]:
        config = self.config.to_dict()

        description = self.description
        name = self.name
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
                "config": config,
                "description": description,
                "name": name,
            }
        )
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
        config = RuntimeConfig.from_dict(d.pop("config"))

        description = d.pop("description")

        name = d.pop("name")

        connectors = []
        _connectors = d.pop("connectors", UNSET)
        for connectors_item_data in _connectors or []:
            connectors_item = AttachedConnector.from_dict(connectors_item_data)

            connectors.append(connectors_item)

        program_name = d.pop("program_name", UNSET)

        new_pipeline_request = cls(
            config=config,
            description=description,
            name=name,
            connectors=connectors,
            program_name=program_name,
        )

        return new_pipeline_request
