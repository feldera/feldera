from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

T = TypeVar("T", bound="UpdateProgramRequest")


@define
class UpdateProgramRequest:
    """Request to update an existing program.

    Attributes:
        code (Union[Unset, None, str]): New SQL code for the program. If absent, existing program code will be
            kept unmodified.
        description (Union[Unset, None, str]): New program description. If absent, existing description will be kept
            unmodified.
        guard (Union[Unset, None, int]): Version number.
        name (Union[Unset, None, str]): New program name. If absent, existing name will be kept unmodified.
    """

    code: Union[Unset, None, str] = UNSET
    description: Union[Unset, None, str] = UNSET
    guard: Union[Unset, None, int] = UNSET
    name: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        code = self.code
        description = self.description
        guard = self.guard
        name = self.name

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if code is not UNSET:
            field_dict["code"] = code
        if description is not UNSET:
            field_dict["description"] = description
        if guard is not UNSET:
            field_dict["guard"] = guard
        if name is not UNSET:
            field_dict["name"] = name

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        code = d.pop("code", UNSET)

        description = d.pop("description", UNSET)

        guard = d.pop("guard", UNSET)

        name = d.pop("name", UNSET)

        update_program_request = cls(
            code=code,
            description=description,
            guard=guard,
            name=name,
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
