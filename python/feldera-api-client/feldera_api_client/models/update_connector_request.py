from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.connector_config import ConnectorConfig


T = TypeVar("T", bound="UpdateConnectorRequest")


@define
class UpdateConnectorRequest:
    """Request to update an existing connector.

    Attributes:
        config (Union[Unset, None, ConnectorConfig]): A data connector's configuration
        description (Union[Unset, None, str]): New connector description. If absent, existing name will be kept
            unmodified.
        name (Union[Unset, None, str]): New connector name. If absent, existing name will be kept unmodified.
    """

    config: Union[Unset, None, "ConnectorConfig"] = UNSET
    description: Union[Unset, None, str] = UNSET
    name: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        config: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict() if self.config else None

        description = self.description
        name = self.name

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if config is not UNSET:
            field_dict["config"] = config
        if description is not UNSET:
            field_dict["description"] = description
        if name is not UNSET:
            field_dict["name"] = name

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.connector_config import ConnectorConfig

        d = src_dict.copy()
        _config = d.pop("config", UNSET)
        config: Union[Unset, None, ConnectorConfig]
        if _config is None:
            config = None
        elif isinstance(_config, Unset):
            config = UNSET
        else:
            config = ConnectorConfig.from_dict(_config)

        description = d.pop("description", UNSET)

        name = d.pop("name", UNSET)

        update_connector_request = cls(
            config=config,
            description=description,
            name=name,
        )

        update_connector_request.additional_properties = d
        return update_connector_request

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
