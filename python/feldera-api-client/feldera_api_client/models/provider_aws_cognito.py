from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="ProviderAwsCognito")


@define
class ProviderAwsCognito:
    """
    Attributes:
        jwk_uri (str):
        login_url (str):
        logout_url (str):
    """

    jwk_uri: str
    login_url: str
    logout_url: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        jwk_uri = self.jwk_uri
        login_url = self.login_url
        logout_url = self.logout_url

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "jwk_uri": jwk_uri,
                "login_url": login_url,
                "logout_url": logout_url,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        jwk_uri = d.pop("jwk_uri")

        login_url = d.pop("login_url")

        logout_url = d.pop("logout_url")

        provider_aws_cognito = cls(
            jwk_uri=jwk_uri,
            login_url=login_url,
            logout_url=logout_url,
        )

        provider_aws_cognito.additional_properties = d
        return provider_aws_cognito

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
