from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

T = TypeVar("T", bound="NewProgramRequest")


@define
class NewProgramRequest:
    """Request to create a new DBSP program.

    Attributes:
        code (str): SQL code of the program. Example: CREATE TABLE Example(name varchar);.
        description (str): Program description. Example: Example description.
        name (str): Program name. Example: Example program.
        jit_mode (Union[Unset, bool]): Compile the program in JIT mode.
    """

    code: str
    description: str
    name: str
    jit_mode: Union[Unset, bool] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        code = self.code
        description = self.description
        name = self.name
        jit_mode = self.jit_mode

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "description": description,
                "name": name,
            }
        )
        if jit_mode is not UNSET:
            field_dict["jit_mode"] = jit_mode

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        code = d.pop("code")

        description = d.pop("description")

        name = d.pop("name")

        jit_mode = d.pop("jit_mode", UNSET)

        new_program_request = cls(
            code=code,
            description=description,
            name=name,
            jit_mode=jit_mode,
        )

        new_program_request.additional_properties = d
        return new_program_request

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
