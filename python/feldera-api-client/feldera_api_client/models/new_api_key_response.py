from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="NewApiKeyResponse")


@define
class NewApiKeyResponse:
    """Response to a successful API key creation.

    Attributes:
        api_key (str): Generated API key. There is no way to
            retrieve this key again from the
            pipeline-manager, so store it securely. Example: 12345678.
        name (str): API key name Example: my-api-key.
    """

    api_key: str
    name: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        api_key = self.api_key
        name = self.name

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "api_key": api_key,
                "name": name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        api_key = d.pop("api_key")

        name = d.pop("name")

        new_api_key_response = cls(
            api_key=api_key,
            name=name,
        )

        new_api_key_response.additional_properties = d
        return new_api_key_response

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
