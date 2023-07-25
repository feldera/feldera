from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="Field")


@define
class Field:
    """A SQL field.

    Attributes:
        name (str):
        nullable (bool):
        type (str):
    """

    name: str
    nullable: bool
    type: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        name = self.name
        nullable = self.nullable
        type = self.type

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "nullable": nullable,
                "type": type,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name")

        nullable = d.pop("nullable")

        type = d.pop("type")

        field = cls(
            name=name,
            nullable=nullable,
            type=type,
        )

        field.additional_properties = d
        return field

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
