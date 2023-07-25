from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

T = TypeVar("T", bound="UpdateProgramRequest")


@define
class UpdateProgramRequest:
    """Update program request.

    Attributes:
        name (str): New name for the program.
        program_id (str): Unique program id.
        code (Union[Unset, None, str]): New SQL code for the program or `None` to keep existing program
            code unmodified.
        description (Union[Unset, str]): New description for the program.
    """

    name: str
    program_id: str
    code: Union[Unset, None, str] = UNSET
    description: Union[Unset, str] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        name = self.name
        program_id = self.program_id
        code = self.code
        description = self.description

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "program_id": program_id,
            }
        )
        if code is not UNSET:
            field_dict["code"] = code
        if description is not UNSET:
            field_dict["description"] = description

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name")

        program_id = d.pop("program_id")

        code = d.pop("code", UNSET)

        description = d.pop("description", UNSET)

        update_program_request = cls(
            name=name,
            program_id=program_id,
            code=code,
            description=description,
        )

        update_program_request.additional_properties = d
        return update_program_request

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
