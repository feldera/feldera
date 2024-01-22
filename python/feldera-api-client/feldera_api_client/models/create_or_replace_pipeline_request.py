from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.attached_connector import AttachedConnector
    from ..models.runtime_config import RuntimeConfig


T = TypeVar("T", bound="CreateOrReplacePipelineRequest")


@define
class CreateOrReplacePipelineRequest:
    """Request to create or replace an existing pipeline.

    Attributes:
        config (RuntimeConfig): Global pipeline configuration settings. This is the publicly
            exposed type for users to configure pipelines.
        description (str): Pipeline description.
        connectors (Union[Unset, None, List['AttachedConnector']]): Attached connectors.
        program_name (Union[Unset, None, str]): Name of the program to create a pipeline for.
    """

    config: "RuntimeConfig"
    description: str
    connectors: Union[Unset, None, List["AttachedConnector"]] = UNSET
    program_name: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        config = self.config.to_dict()

        description = self.description
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
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "config": config,
                "description": description,
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

        connectors = []
        _connectors = d.pop("connectors", UNSET)
        for connectors_item_data in _connectors or []:
            connectors_item = AttachedConnector.from_dict(connectors_item_data)

            connectors.append(connectors_item)

        program_name = d.pop("program_name", UNSET)

        create_or_replace_pipeline_request = cls(
            config=config,
            description=description,
            connectors=connectors,
            program_name=program_name,
        )

        create_or_replace_pipeline_request.additional_properties = d
        return create_or_replace_pipeline_request

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
