from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

import attr

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.transport_config_config import TransportConfigConfig


T = TypeVar("T", bound="TransportConfig")


@attr.s(auto_attribs=True)
class TransportConfig:
    """Transport endpoint configuration.

    Attributes:
        name (str): Data transport name, e.g., "file", "kafka", "kinesis", etc.
        config (Union[Unset, TransportConfigConfig]): Transport-specific endpoint configuration passed to
            [`OutputTransport::new_endpoint`](`crate::OutputTransport::new_endpoint`)
            and
            [`InputTransport::new_endpoint`](`crate::InputTransport::new_endpoint`).
    """

    name: str
    config: Union[Unset, "TransportConfigConfig"] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        name = self.name
        config: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.transport_config_config import TransportConfigConfig

        d = src_dict.copy()
        name = d.pop("name")

        _config = d.pop("config", UNSET)
        config: Union[Unset, TransportConfigConfig]
        if isinstance(_config, Unset):
            config = UNSET
        else:
            config = TransportConfigConfig.from_dict(_config)

        transport_config = cls(
            name=name,
            config=config,
        )

        transport_config.additional_properties = d
        return transport_config

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
