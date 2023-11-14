from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.kafka_input_ft_config_consumer_options import KafkaInputFtConfigConsumerOptions
    from ..models.kafka_input_ft_config_producer_options import KafkaInputFtConfigProducerOptions


T = TypeVar("T", bound="KafkaInputFtConfig")


@define
class KafkaInputFtConfig:
    """Fault tolerance configuration for Kafka input connector.

    Attributes:
        consumer_options (Union[Unset, KafkaInputFtConfigConsumerOptions]): Options passed to `rdkafka` for consumers
            only, as documented at
            [`librdkafka`
            options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

            These options override `kafka_options` for consumers, and may be empty.
        create_missing_index (Union[Unset, None, bool]): If this is true or unset, then the connector will create
            missing index
            topics as needed.  If this is false, then a missing index topic is a
            fatal error.
        index_suffix (Union[Unset, None, str]): Suffix to append to each data topic name, to give the name of a topic
            that the connector uses for recording the division of the corresponding
            data topic into steps.  Defaults to `_input-index`.

            An index topic must have the same number of partitions as its
            corresponding data topic.

            If two or more durable Kafka endpoints read from overlapping sets of
            topics, they must specify different `index_suffix` values.
        max_step_bytes (Union[Unset, None, int]): Maximum number of bytes in a step.  Any individual message bigger than
            this will be given a step of its own.
        max_step_messages (Union[Unset, None, int]): Maximum number of messages in a step.
        producer_options (Union[Unset, KafkaInputFtConfigProducerOptions]): Options passed to `rdkafka` for producers
            only, as documented at
            [`librdkafka`
            options](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

            These options override `kafka_options` for producers, and may be empty.
    """

    consumer_options: Union[Unset, "KafkaInputFtConfigConsumerOptions"] = UNSET
    create_missing_index: Union[Unset, None, bool] = UNSET
    index_suffix: Union[Unset, None, str] = UNSET
    max_step_bytes: Union[Unset, None, int] = UNSET
    max_step_messages: Union[Unset, None, int] = UNSET
    producer_options: Union[Unset, "KafkaInputFtConfigProducerOptions"] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        consumer_options: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.consumer_options, Unset):
            consumer_options = self.consumer_options.to_dict()

        create_missing_index = self.create_missing_index
        index_suffix = self.index_suffix
        max_step_bytes = self.max_step_bytes
        max_step_messages = self.max_step_messages
        producer_options: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.producer_options, Unset):
            producer_options = self.producer_options.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if consumer_options is not UNSET:
            field_dict["consumer_options"] = consumer_options
        if create_missing_index is not UNSET:
            field_dict["create_missing_index"] = create_missing_index
        if index_suffix is not UNSET:
            field_dict["index_suffix"] = index_suffix
        if max_step_bytes is not UNSET:
            field_dict["max_step_bytes"] = max_step_bytes
        if max_step_messages is not UNSET:
            field_dict["max_step_messages"] = max_step_messages
        if producer_options is not UNSET:
            field_dict["producer_options"] = producer_options

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.kafka_input_ft_config_consumer_options import KafkaInputFtConfigConsumerOptions
        from ..models.kafka_input_ft_config_producer_options import KafkaInputFtConfigProducerOptions

        d = src_dict.copy()
        _consumer_options = d.pop("consumer_options", UNSET)
        consumer_options: Union[Unset, KafkaInputFtConfigConsumerOptions]
        if isinstance(_consumer_options, Unset):
            consumer_options = UNSET
        else:
            consumer_options = KafkaInputFtConfigConsumerOptions.from_dict(_consumer_options)

        create_missing_index = d.pop("create_missing_index", UNSET)

        index_suffix = d.pop("index_suffix", UNSET)

        max_step_bytes = d.pop("max_step_bytes", UNSET)

        max_step_messages = d.pop("max_step_messages", UNSET)

        _producer_options = d.pop("producer_options", UNSET)
        producer_options: Union[Unset, KafkaInputFtConfigProducerOptions]
        if isinstance(_producer_options, Unset):
            producer_options = UNSET
        else:
            producer_options = KafkaInputFtConfigProducerOptions.from_dict(_producer_options)

        kafka_input_ft_config = cls(
            consumer_options=consumer_options,
            create_missing_index=create_missing_index,
            index_suffix=index_suffix,
            max_step_bytes=max_step_bytes,
            max_step_messages=max_step_messages,
            producer_options=producer_options,
        )

        kafka_input_ft_config.additional_properties = d
        return kafka_input_ft_config

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
