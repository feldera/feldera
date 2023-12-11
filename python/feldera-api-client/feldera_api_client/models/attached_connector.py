from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="AttachedConnector")


@define
class AttachedConnector:
    """Format to add attached connectors during a config update.

    Attributes:
        connector_name (str): The name of the connector to attach.
        is_input (bool): True for input connectors, false for output connectors.
        name (str): A unique identifier for this attachement.
        relation_name (str): The table or view this connector is attached to. Unquoted
            table/view names in the SQL program need to be capitalized
            here. Quoted table/view names have to exactly match the
            casing from the SQL program.
    """

    connector_name: str
    is_input: bool
    name: str
    relation_name: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        connector_name = self.connector_name
        is_input = self.is_input
        name = self.name
        relation_name = self.relation_name

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connector_name": connector_name,
                "is_input": is_input,
                "name": name,
                "relation_name": relation_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        connector_name = d.pop("connector_name")

        is_input = d.pop("is_input")

        name = d.pop("name")

        relation_name = d.pop("relation_name")

        attached_connector = cls(
            connector_name=connector_name,
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
