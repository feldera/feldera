from typing import Any, Dict, List, Type, TypeVar

import attr

T = TypeVar("T", bound="ConnectorDescr")


@attr.s(auto_attribs=True)
class ConnectorDescr:
    """Connector descriptor.

    Attributes:
        config (str):
        connector_id (str): Unique connector id.
        description (str):
        name (str):
    """

    config: str
    connector_id: str
    description: str
    name: str
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        config = self.config
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
        d = src_dict.copy()
        config = d.pop("config")

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
