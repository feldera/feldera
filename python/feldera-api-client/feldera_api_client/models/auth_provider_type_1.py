from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.provider_google_identity import ProviderGoogleIdentity


T = TypeVar("T", bound="AuthProviderType1")


@define
class AuthProviderType1:
    """
    Attributes:
        google_identity (ProviderGoogleIdentity):
    """

    google_identity: "ProviderGoogleIdentity"
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        google_identity = self.google_identity.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "GoogleIdentity": google_identity,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.provider_google_identity import ProviderGoogleIdentity

        d = src_dict.copy()
        google_identity = ProviderGoogleIdentity.from_dict(d.pop("GoogleIdentity"))

        auth_provider_type_1 = cls(
            google_identity=google_identity,
        )

        auth_provider_type_1.additional_properties = d
        return auth_provider_type_1

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
