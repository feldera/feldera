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
        overwrite_existing (Union[Unset, bool]): Overwrite existing program with the same name, if any.
    """

    code: str
    description: str
    name: str
    overwrite_existing: Union[Unset, bool] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        code = self.code
        description = self.description
        name = self.name
        overwrite_existing = self.overwrite_existing

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "code": code,
                "description": description,
                "name": name,
            }
        )
        if overwrite_existing is not UNSET:
            field_dict["overwrite_existing"] = overwrite_existing

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        code = d.pop("code")

        description = d.pop("description")

        name = d.pop("name")

        overwrite_existing = d.pop("overwrite_existing", UNSET)

        new_program_request = cls(
            code=code,
            description=description,
            name=name,
            overwrite_existing=overwrite_existing,
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
