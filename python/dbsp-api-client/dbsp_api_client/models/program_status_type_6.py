from typing import Any, Dict, List, Type, TypeVar

import attr

T = TypeVar("T", bound="ProgramStatusType6")


@attr.s(auto_attribs=True)
class ProgramStatusType6:
    """
    Attributes:
        rust_error (str): Rust compiler returned an error.
    """

    rust_error: str
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        rust_error = self.rust_error

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "RustError": rust_error,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        rust_error = d.pop("RustError")

        program_status_type_6 = cls(
            rust_error=rust_error,
        )

        program_status_type_6.additional_properties = d
        return program_status_type_6

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
