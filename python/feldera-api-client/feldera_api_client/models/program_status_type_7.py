from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="ProgramStatusType7")


@define
class ProgramStatusType7:
    """
    Attributes:
        system_error (str): System/OS returned an error when trying to invoke commands.
    """

    system_error: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        system_error = self.system_error

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "SystemError": system_error,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        system_error = d.pop("SystemError")

        program_status_type_7 = cls(
            system_error=system_error,
        )

        program_status_type_7.additional_properties = d
        return program_status_type_7

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
