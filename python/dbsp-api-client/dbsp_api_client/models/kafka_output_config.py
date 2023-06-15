from typing import Any, Dict, List, Type, TypeVar, Union

import attr

from ..models.kafka_output_config_log_level import KafkaOutputConfigLogLevel
from ..types import UNSET, Unset

T = TypeVar("T", bound="KafkaOutputConfig")


@attr.s(auto_attribs=True)
class KafkaOutputConfig:
    """
    Attributes:
        topic (str):
        log_level (Union[Unset, KafkaOutputConfigLogLevel]): Kafka logging levels.
        max_inflight_messages (Union[Unset, int]): Maximum number of unacknowledged messages buffered by the Kafka
            producer.

            Kafka producer buffers outgoing messages until it receives an
            acknowledgement from the broker.  This configuration parameter
            bounds the number of unacknowledged messages.  When the number of
            unacknowledged messages reaches this limit, sending of a new message
            blocks until additional acknowledgements arrive from the broker.

            Defaults to 1000.
    """

    topic: str
    log_level: Union[Unset, KafkaOutputConfigLogLevel] = UNSET
    max_inflight_messages: Union[Unset, int] = UNSET
    additional_properties: Dict[str, str] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        topic = self.topic
        log_level: Union[Unset, str] = UNSET
        if not isinstance(self.log_level, Unset):
            log_level = self.log_level.value

        max_inflight_messages = self.max_inflight_messages

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "topic": topic,
            }
        )
        if log_level is not UNSET:
            field_dict["log_level"] = log_level
        if max_inflight_messages is not UNSET:
            field_dict["max_inflight_messages"] = max_inflight_messages

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        topic = d.pop("topic")

        _log_level = d.pop("log_level", UNSET)
        log_level: Union[Unset, KafkaOutputConfigLogLevel]
        if isinstance(_log_level, Unset):
            log_level = UNSET
        else:
            log_level = KafkaOutputConfigLogLevel(_log_level)

        max_inflight_messages = d.pop("max_inflight_messages", UNSET)

        kafka_output_config = cls(
            topic=topic,
            log_level=log_level,
            max_inflight_messages=max_inflight_messages,
        )

        kafka_output_config.additional_properties = d
        return kafka_output_config

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
