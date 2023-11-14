from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.kafka_output_ft_config_consumer_options import KafkaOutputFtConfigConsumerOptions
    from ..models.kafka_output_ft_config_producer_options import KafkaOutputFtConfigProducerOptions


T = TypeVar("T", bound="KafkaOutputFtConfig")


@define
class KafkaOutputFtConfig:
    """Fault tolerance configuration for Kafka output connector.

    Attributes:
        consumer_options (Union[Unset, KafkaOutputFtConfigConsumerOptions]): Options passed to `rdkafka` for consumers
            only, as documented at
            [`librdkafka`
            options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

            These options override `kafka_options` for consumers, and may be empty.
        producer_options (Union[Unset, KafkaOutputFtConfigProducerOptions]): Options passed to `rdkafka` for producers
            only, as documented at
            [`librdkafka`
            options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

            These options override `kafka_options` for producers, and may be empty.
    """

    consumer_options: Union[Unset, "KafkaOutputFtConfigConsumerOptions"] = UNSET
    producer_options: Union[Unset, "KafkaOutputFtConfigProducerOptions"] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        consumer_options: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.consumer_options, Unset):
            consumer_options = self.consumer_options.to_dict()

        producer_options: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.producer_options, Unset):
            producer_options = self.producer_options.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if consumer_options is not UNSET:
            field_dict["consumer_options"] = consumer_options
        if producer_options is not UNSET:
            field_dict["producer_options"] = producer_options

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.kafka_output_ft_config_consumer_options import KafkaOutputFtConfigConsumerOptions
        from ..models.kafka_output_ft_config_producer_options import KafkaOutputFtConfigProducerOptions

        d = src_dict.copy()
        _consumer_options = d.pop("consumer_options", UNSET)
        consumer_options: Union[Unset, KafkaOutputFtConfigConsumerOptions]
        if isinstance(_consumer_options, Unset):
            consumer_options = UNSET
        else:
            consumer_options = KafkaOutputFtConfigConsumerOptions.from_dict(_consumer_options)

        _producer_options = d.pop("producer_options", UNSET)
        producer_options: Union[Unset, KafkaOutputFtConfigProducerOptions]
        if isinstance(_producer_options, Unset):
            producer_options = UNSET
        else:
            producer_options = KafkaOutputFtConfigProducerOptions.from_dict(_producer_options)

        kafka_output_ft_config = cls(
            consumer_options=consumer_options,
            producer_options=producer_options,
        )

        kafka_output_ft_config.additional_properties = d
        return kafka_output_ft_config

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
