from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.provider_aws_cognito import ProviderAwsCognito


T = TypeVar("T", bound="AuthProviderType0")


@define
class AuthProviderType0:
    """
    Attributes:
        aws_cognito (ProviderAwsCognito):
    """

    aws_cognito: "ProviderAwsCognito"
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        aws_cognito = self.aws_cognito.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "AwsCognito": aws_cognito,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.provider_aws_cognito import ProviderAwsCognito

        d = src_dict.copy()
        aws_cognito = ProviderAwsCognito.from_dict(d.pop("AwsCognito"))

        auth_provider_type_0 = cls(
            aws_cognito=aws_cognito,
        )

        auth_provider_type_0.additional_properties = d
        return auth_provider_type_0

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
