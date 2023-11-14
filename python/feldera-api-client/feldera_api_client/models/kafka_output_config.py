from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..models.kafka_log_level import KafkaLogLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.kafka_output_ft_config import KafkaOutputFtConfig


T = TypeVar("T", bound="KafkaOutputConfig")


@define
class KafkaOutputConfig:
    """Configuration for writing data to a Kafka topic with `OutputTransport`.

    Attributes:
        topic (str): Topic to write to.
        fault_tolerance (Union[Unset, None, KafkaOutputFtConfig]): Fault tolerance configuration for Kafka output
            connector.
        initialization_timeout_secs (Union[Unset, int]): Maximum timeout in seconds to wait for the endpoint to connect
            to
            a Kafka broker.

            Defaults to 10.
        log_level (Union[Unset, None, KafkaLogLevel]): Kafka logging levels.
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
    fault_tolerance: Union[Unset, None, "KafkaOutputFtConfig"] = UNSET
    initialization_timeout_secs: Union[Unset, int] = UNSET
    log_level: Union[Unset, None, KafkaLogLevel] = UNSET
    max_inflight_messages: Union[Unset, int] = UNSET
    additional_properties: Dict[str, str] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        topic = self.topic
        fault_tolerance: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.fault_tolerance, Unset):
            fault_tolerance = self.fault_tolerance.to_dict() if self.fault_tolerance else None

        initialization_timeout_secs = self.initialization_timeout_secs
        log_level: Union[Unset, None, str] = UNSET
        if not isinstance(self.log_level, Unset):
            log_level = self.log_level.value if self.log_level else None

        max_inflight_messages = self.max_inflight_messages

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "topic": topic,
            }
        )
        if fault_tolerance is not UNSET:
            field_dict["fault_tolerance"] = fault_tolerance
        if initialization_timeout_secs is not UNSET:
            field_dict["initialization_timeout_secs"] = initialization_timeout_secs
        if log_level is not UNSET:
            field_dict["log_level"] = log_level
        if max_inflight_messages is not UNSET:
            field_dict["max_inflight_messages"] = max_inflight_messages

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.kafka_output_ft_config import KafkaOutputFtConfig

        d = src_dict.copy()
        topic = d.pop("topic")

        _fault_tolerance = d.pop("fault_tolerance", UNSET)
        fault_tolerance: Union[Unset, None, KafkaOutputFtConfig]
        if _fault_tolerance is None:
            fault_tolerance = None
        elif isinstance(_fault_tolerance, Unset):
            fault_tolerance = UNSET
        else:
            fault_tolerance = KafkaOutputFtConfig.from_dict(_fault_tolerance)

        initialization_timeout_secs = d.pop("initialization_timeout_secs", UNSET)

        _log_level = d.pop("log_level", UNSET)
        log_level: Union[Unset, None, KafkaLogLevel]
        if _log_level is None:
            log_level = None
        elif isinstance(_log_level, Unset):
            log_level = UNSET
        else:
            log_level = KafkaLogLevel(_log_level)

        max_inflight_messages = d.pop("max_inflight_messages", UNSET)

        kafka_output_config = cls(
            topic=topic,
            fault_tolerance=fault_tolerance,
            initialization_timeout_secs=initialization_timeout_secs,
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
