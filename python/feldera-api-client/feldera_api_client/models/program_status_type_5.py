from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.sql_compiler_message import SqlCompilerMessage


T = TypeVar("T", bound="ProgramStatusType5")


@define
class ProgramStatusType5:
    """
    Attributes:
        sql_error (List['SqlCompilerMessage']): SQL compiler returned an error.
    """

    sql_error: List["SqlCompilerMessage"]
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        sql_error = []
        for sql_error_item_data in self.sql_error:
            sql_error_item = sql_error_item_data.to_dict()

            sql_error.append(sql_error_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "SqlError": sql_error,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.sql_compiler_message import SqlCompilerMessage

        d = src_dict.copy()
        sql_error = []
        _sql_error = d.pop("SqlError")
        for sql_error_item_data in _sql_error:
            sql_error_item = SqlCompilerMessage.from_dict(sql_error_item_data)

            sql_error.append(sql_error_item)

        program_status_type_5 = cls(
            sql_error=sql_error,
        )

        program_status_type_5.additional_properties = d
        return program_status_type_5

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
