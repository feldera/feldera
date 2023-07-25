from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="AttachedConnector")


@define
class AttachedConnector:
    """Format to add attached connectors during a config update.

    Attributes:
        connector_id (str): Unique connector id.
        is_input (bool): Is this an input or an output?
        name (str): A unique identifier for this attachement.
        relation_name (str): The table or view this connector is attached to.
    """

    connector_id: str
    is_input: bool
    name: str
    relation_name: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        connector_id = self.connector_id
        is_input = self.is_input
        name = self.name
        relation_name = self.relation_name

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connector_id": connector_id,
                "is_input": is_input,
                "name": name,
                "relation_name": relation_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        connector_id = d.pop("connector_id")

        is_input = d.pop("is_input")

        name = d.pop("name")

        relation_name = d.pop("relation_name")

        attached_connector = cls(
            connector_id=connector_id,
            is_input=is_input,
            name=name,
            relation_name=relation_name,
        )

        attached_connector.additional_properties = d
        return attached_connector

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
