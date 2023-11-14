from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union, cast

from attrs import define, field

from ..models.kafka_log_level import KafkaLogLevel
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.kafka_input_ft_config import KafkaInputFtConfig


T = TypeVar("T", bound="KafkaInputConfig")


@define
class KafkaInputConfig:
    """Configuration for reading data from Kafka topics with `InputTransport`.

    Attributes:
        topics (List[str]): List of topics to subscribe to.
        fault_tolerance (Union[Unset, None, KafkaInputFtConfig]): Fault tolerance configuration for Kafka input
            connector.
        group_join_timeout_secs (Union[Unset, int]): Maximum timeout in seconds to wait for the endpoint to join the
            Kafka
            consumer group during initialization.
        log_level (Union[Unset, None, KafkaLogLevel]): Kafka logging levels.
    """

    topics: List[str]
    fault_tolerance: Union[Unset, None, "KafkaInputFtConfig"] = UNSET
    group_join_timeout_secs: Union[Unset, int] = UNSET
    log_level: Union[Unset, None, KafkaLogLevel] = UNSET
    additional_properties: Dict[str, str] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        topics = self.topics

        fault_tolerance: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.fault_tolerance, Unset):
            fault_tolerance = self.fault_tolerance.to_dict() if self.fault_tolerance else None

        group_join_timeout_secs = self.group_join_timeout_secs
        log_level: Union[Unset, None, str] = UNSET
        if not isinstance(self.log_level, Unset):
            log_level = self.log_level.value if self.log_level else None

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "topics": topics,
            }
        )
        if fault_tolerance is not UNSET:
            field_dict["fault_tolerance"] = fault_tolerance
        if group_join_timeout_secs is not UNSET:
            field_dict["group_join_timeout_secs"] = group_join_timeout_secs
        if log_level is not UNSET:
            field_dict["log_level"] = log_level

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.kafka_input_ft_config import KafkaInputFtConfig

        d = src_dict.copy()
        topics = cast(List[str], d.pop("topics"))

        _fault_tolerance = d.pop("fault_tolerance", UNSET)
        fault_tolerance: Union[Unset, None, KafkaInputFtConfig]
        if _fault_tolerance is None:
            fault_tolerance = None
        elif isinstance(_fault_tolerance, Unset):
            fault_tolerance = UNSET
        else:
            fault_tolerance = KafkaInputFtConfig.from_dict(_fault_tolerance)

        group_join_timeout_secs = d.pop("group_join_timeout_secs", UNSET)

        _log_level = d.pop("log_level", UNSET)
        log_level: Union[Unset, None, KafkaLogLevel]
        if _log_level is None:
            log_level = None
        elif isinstance(_log_level, Unset):
            log_level = UNSET
        else:
            log_level = KafkaLogLevel(_log_level)

        kafka_input_config = cls(
            topics=topics,
            fault_tolerance=fault_tolerance,
            group_join_timeout_secs=group_join_timeout_secs,
            log_level=log_level,
        )

        kafka_input_config.additional_properties = d
        return kafka_input_config

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
