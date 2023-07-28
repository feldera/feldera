from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.connector_config import ConnectorConfig


T = TypeVar("T", bound="ConnectorDescr")


@define
class ConnectorDescr:
    """Connector descriptor.

    Attributes:
        config (ConnectorConfig): A data connector's configuration
        connector_id (str): Unique connector id.
        description (str):
        name (str):
    """

    config: "ConnectorConfig"
    connector_id: str
    description: str
    name: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        config = self.config.to_dict()

        connector_id = self.connector_id
        description = self.description
        name = self.name

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "config": config,
                "connector_id": connector_id,
                "description": description,
                "name": name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.connector_config import ConnectorConfig

        d = src_dict.copy()
        config = ConnectorConfig.from_dict(d.pop("config"))

        connector_id = d.pop("connector_id")

        description = d.pop("description")

        name = d.pop("name")

        connector_descr = cls(
            config=config,
            connector_id=connector_id,
            description=description,
            name=name,
        )

        connector_descr.additional_properties = d
        return connector_descr

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
