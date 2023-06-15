from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

import attr

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.format_config_config import FormatConfigConfig


T = TypeVar("T", bound="FormatConfig")


@attr.s(auto_attribs=True)
class FormatConfig:
    """Data format specification used to parse raw data received from the
    endpoint or to encode data sent to the endpoint.

        Attributes:
            name (str): Format name, e.g., "csv", "json", "bincode", etc.
            config (Union[Unset, FormatConfigConfig]): Format-specific parser or encoder configuration.
    """

    name: str
    config: Union[Unset, "FormatConfigConfig"] = UNSET
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
        from ..models.format_config_config import FormatConfigConfig

        d = src_dict.copy()
        name = d.pop("name")

        _config = d.pop("config", UNSET)
        config: Union[Unset, FormatConfigConfig]
        if isinstance(_config, Unset):
            config = UNSET
        else:
            config = FormatConfigConfig.from_dict(_config)

        format_config = cls(
            name=name,
            config=config,
        )

        format_config.additional_properties = d
        return format_config

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
