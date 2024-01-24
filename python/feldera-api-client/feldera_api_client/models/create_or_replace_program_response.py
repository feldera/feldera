from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="CreateOrReplaceProgramResponse")


@define
class CreateOrReplaceProgramResponse:
    """Response to a create or replace program request.

    Attributes:
        program_id (str): Unique program id.
        version (int): Version number.
    """

    program_id: str
    version: int
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        program_id = self.program_id
        version = self.version

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "program_id": program_id,
                "version": version,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        program_id = d.pop("program_id")

        version = d.pop("version")

        create_or_replace_program_response = cls(
            program_id=program_id,
            version=version,
        )

        create_or_replace_program_response.additional_properties = d
        return create_or_replace_program_response

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
