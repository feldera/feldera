from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.column_type import ColumnType


T = TypeVar("T", bound="Field")


@define
class Field:
    """A SQL field.

    Matches the Calcite JSON format.

        Attributes:
            columntype (ColumnType): A SQL column type description.

                Matches the Calcite JSON format.
            name (str):
    """

    columntype: "ColumnType"
    name: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        columntype = self.columntype.to_dict()

        name = self.name

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "columntype": columntype,
                "name": name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.column_type import ColumnType

        d = src_dict.copy()
        columntype = ColumnType.from_dict(d.pop("columntype"))

        name = d.pop("name")

        field = cls(
            columntype=columntype,
            name=name,
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
