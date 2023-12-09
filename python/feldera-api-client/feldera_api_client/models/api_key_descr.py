from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

from ..models.api_permission import ApiPermission

T = TypeVar("T", bound="ApiKeyDescr")


@define
class ApiKeyDescr:
    """Api Key descriptor.

    Attributes:
        name (str):
        scopes (List[ApiPermission]):
    """

    name: str
    scopes: List[ApiPermission]
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        name = self.name
        scopes = []
        for scopes_item_data in self.scopes:
            scopes_item = scopes_item_data.value

            scopes.append(scopes_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "scopes": scopes,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name")

        scopes = []
        _scopes = d.pop("scopes")
        for scopes_item_data in _scopes:
            scopes_item = ApiPermission(scopes_item_data)

            scopes.append(scopes_item)

        api_key_descr = cls(
            name=name,
            scopes=scopes,
        )

        api_key_descr.additional_properties = d
        return api_key_descr

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
