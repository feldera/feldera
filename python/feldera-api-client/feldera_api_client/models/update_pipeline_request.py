from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

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
        config (Union[Unset, None, RuntimeConfig]): Global pipeline configuration settings.
        connectors (Union[Unset, None, List['AttachedConnector']]): Attached connectors.

            - If absent, existing connectors will be kept unmodified.

            - If present all existing connectors will be replaced with the new
            specified list.
        program_id (Union[Unset, None, str]): Unique program id.
    """

    description: str
    name: str
    config: Union[Unset, None, "RuntimeConfig"] = UNSET
    connectors: Union[Unset, None, List["AttachedConnector"]] = UNSET
    program_id: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

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

        program_id = self.program_id

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
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
        if program_id is not UNSET:
            field_dict["program_id"] = program_id

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

        program_id = d.pop("program_id", UNSET)

        update_pipeline_request = cls(
            description=description,
            name=name,
            config=config,
            connectors=connectors,
            program_id=program_id,
        )

        update_pipeline_request.additional_properties = d
        return update_pipeline_request

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
