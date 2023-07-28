from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ColumnType")


@define
class ColumnType:
    """A SQL column type description.

    Matches the Calcite JSON format.

        Attributes:
            nullable (bool): Does the type accept NULL values?
            type (str): Identifier for the type (e.g., `VARCHAR`, `BIGINT`, `ARRAY` etc.)
            component (Union[Unset, None, ColumnType]): A SQL column type description.

                Matches the Calcite JSON format.
            precision (Union[Unset, None, int]): Precision of the type.

                # Examples
                - `VARCHAR` sets precision to `-1`.
                - `VARCHAR(255)` sets precision to `255`.
                - `BIGINT`, `DATE`, `FLOAT`, `DOUBLE`, `GEOMETRY`, etc. sets precision to None
                - `TIME`, `TIMESTAMP` set precision to `0`.
            scale (Union[Unset, None, int]): The scale of the type.

                # Example
                - `DECIMAL(1,2)` sets scale to `2`.
    """

    nullable: bool
    type: str
    component: Union[Unset, None, "ColumnType"] = UNSET
    precision: Union[Unset, None, int] = UNSET
    scale: Union[Unset, None, int] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        nullable = self.nullable
        type = self.type
        component: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.component, Unset):
            component = self.component.to_dict() if self.component else None

        precision = self.precision
        scale = self.scale

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "nullable": nullable,
                "type": type,
            }
        )
        if component is not UNSET:
            field_dict["component"] = component
        if precision is not UNSET:
            field_dict["precision"] = precision
        if scale is not UNSET:
            field_dict["scale"] = scale

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        nullable = d.pop("nullable")

        type = d.pop("type")

        _component = d.pop("component", UNSET)
        component: Union[Unset, None, ColumnType]
        if _component is None:
            component = None
        elif isinstance(_component, Unset):
            component = UNSET
        else:
            component = ColumnType.from_dict(_component)

        precision = d.pop("precision", UNSET)

        scale = d.pop("scale", UNSET)

        column_type = cls(
            nullable=nullable,
            type=type,
            component=component,
            precision=precision,
            scale=scale,
        )

        column_type.additional_properties = d
        return column_type

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
