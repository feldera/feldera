from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="KafkaOutputFtConfigProducerOptions")


@define
class KafkaOutputFtConfigProducerOptions:
    """Options passed to `rdkafka` for producers only, as documented at
    [`librdkafka`
    options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

    These options override `kafka_options` for producers, and may be empty.

    """

    additional_properties: Dict[str, str] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        kafka_output_ft_config_producer_options = cls()

        kafka_output_ft_config_producer_options.additional_properties = d
        return kafka_output_ft_config_producer_options

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> str:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: str) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
