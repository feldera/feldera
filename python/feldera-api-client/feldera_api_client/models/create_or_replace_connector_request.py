from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.connector_config import ConnectorConfig


T = TypeVar("T", bound="CreateOrReplaceConnectorRequest")


@define
class CreateOrReplaceConnectorRequest:
    """Request to create or replace a connector.

    Attributes:
        config (ConnectorConfig): A data connector's configuration
        description (str): New connector description.
    """

    config: "ConnectorConfig"
    description: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        config = self.config.to_dict()

        description = self.description

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "config": config,
                "description": description,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.connector_config import ConnectorConfig

        d = src_dict.copy()
        config = ConnectorConfig.from_dict(d.pop("config"))

        description = d.pop("description")

        create_or_replace_connector_request = cls(
            config=config,
            description=description,
        )

        create_or_replace_connector_request.additional_properties = d
        return create_or_replace_connector_request

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
