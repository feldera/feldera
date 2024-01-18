from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="CreateOrReplaceServiceResponse")


@define
class CreateOrReplaceServiceResponse:
    """Response to a create or replace service request.

    Attributes:
        service_id (str): Unique service id.
    """

    service_id: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        service_id = self.service_id

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "service_id": service_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        service_id = d.pop("service_id")

        create_or_replace_service_response = cls(
            service_id=service_id,
        )

        create_or_replace_service_response.additional_properties = d
        return create_or_replace_service_response

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
