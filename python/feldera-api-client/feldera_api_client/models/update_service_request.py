from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.service_config_type_0 import ServiceConfigType0
    from ..models.service_config_type_1 import ServiceConfigType1


T = TypeVar("T", bound="UpdateServiceRequest")


@define
class UpdateServiceRequest:
    """Request to update an existing service.

    Attributes:
        description (str): New service description.
        config (Union['ServiceConfigType0', 'ServiceConfigType1', None, Unset]): A service's configuration.
    """

    description: str
    config: Union["ServiceConfigType0", "ServiceConfigType1", None, Unset] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from ..models.service_config_type_0 import ServiceConfigType0

        description = self.description
        config: Union[Dict[str, Any], None, Unset]
        if isinstance(self.config, Unset):
            config = UNSET
        elif self.config is None:
            config = None

        elif isinstance(self.config, ServiceConfigType0):
            config = self.config.to_dict()

        else:
            config = self.config.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "description": description,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.service_config_type_0 import ServiceConfigType0
        from ..models.service_config_type_1 import ServiceConfigType1

        d = src_dict.copy()
        description = d.pop("description")

        def _parse_config(data: object) -> Union["ServiceConfigType0", "ServiceConfigType1", None, Unset]:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_service_config_type_0 = ServiceConfigType0.from_dict(data)

                return componentsschemas_service_config_type_0
            except:  # noqa: E722
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_service_config_type_1 = ServiceConfigType1.from_dict(data)

            return componentsschemas_service_config_type_1

        config = _parse_config(d.pop("config", UNSET))

        update_service_request = cls(
            description=description,
            config=config,
        )

        update_service_request.additional_properties = d
        return update_service_request

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
