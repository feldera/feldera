from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.service_config_type_0 import ServiceConfigType0


T = TypeVar("T", bound="CreateOrReplaceServiceRequest")


@define
class CreateOrReplaceServiceRequest:
    """Request to create or replace a service.

    Attributes:
        config ('ServiceConfigType0'): Service configuration for the API

            A Service is an API object, with as one of its properties its config.
            The config is a variant of this enumeration, and is stored serialized
            in the database.

            How a service configuration is applied can vary by connector, e.g., some
            might have options that are mutually exclusive whereas others might be
            defaults that can be overriden.
        description (str): Service description.
    """

    config: "ServiceConfigType0"
    description: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from ..models.service_config_type_0 import ServiceConfigType0

        config: Dict[str, Any]

        if isinstance(self.config, ServiceConfigType0):
            config = self.config.to_dict()

        description = self.description

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "config": config,
                "description": description,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.service_config_type_0 import ServiceConfigType0

        d = src_dict.copy()

        def _parse_config(data: object) -> "ServiceConfigType0":
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_service_config_type_0 = ServiceConfigType0.from_dict(data)

            return componentsschemas_service_config_type_0

        config = _parse_config(d.pop("config"))

        description = d.pop("description")

        create_or_replace_service_request = cls(
            config=config,
            description=description,
        )

        create_or_replace_service_request.additional_properties = d
        return create_or_replace_service_request

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
