from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

import attr

if TYPE_CHECKING:
    from ..models.program_descr import ProgramDescr


T = TypeVar("T", bound="ProgramCodeResponse")


@attr.s(auto_attribs=True)
class ProgramCodeResponse:
    """Response to a program code request.

    Attributes:
        code (str): Program code.
        program (ProgramDescr): Program descriptor.
    """

    code: str
    program: "ProgramDescr"
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        code = self.code
        program = self.program.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "program": program,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.program_descr import ProgramDescr

        d = src_dict.copy()
        code = d.pop("code")

        program = ProgramDescr.from_dict(d.pop("program"))

        program_code_response = cls(
            code=code,
            program=program,
        )

        program_code_response.additional_properties = d
        return program_code_response

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
