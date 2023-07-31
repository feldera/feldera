from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.connector_config import ConnectorConfig


T = TypeVar("T", bound="UpdateConnectorRequest")


@define
class UpdateConnectorRequest:
    """Request to update an existing data-connector.

    Attributes:
        description (str): New connector description.
        name (str): New connector name.
        config (Union[Unset, None, ConnectorConfig]): A data connector's configuration
    """

    description: str
    name: str
    config: Union[Unset, None, "ConnectorConfig"] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        description = self.description
        name = self.name
        config: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict() if self.config else None

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

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.connector_config import ConnectorConfig

        d = src_dict.copy()
        description = d.pop("description")

        name = d.pop("name")

        _config = d.pop("config", UNSET)
        config: Union[Unset, None, ConnectorConfig]
        if _config is None:
            config = None
        elif isinstance(_config, Unset):
            config = UNSET
        else:
            config = ConnectorConfig.from_dict(_config)

        update_connector_request = cls(
            description=description,
            name=name,
            config=config,
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
