from typing import Any, Dict, List, Type, TypeVar, Union, cast

from attrs import define, field

from ..models.kafka_input_config_log_level import KafkaInputConfigLogLevel
from ..types import UNSET, Unset

T = TypeVar("T", bound="KafkaInputConfig")


@define
class KafkaInputConfig:
    """
    Attributes:
        topics (List[str]):
        group_join_timeout_secs (Union[Unset, int]): Maximum timeout in seconds to wait for the endpoint to join the
            Kafka consumer group during initialization.
        log_level (Union[Unset, KafkaInputConfigLogLevel]): Kafka logging levels.
    """

    topics: List[str]
    group_join_timeout_secs: Union[Unset, int] = UNSET
    log_level: Union[Unset, KafkaInputConfigLogLevel] = UNSET
    additional_properties: Dict[str, str] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        topics = self.topics

        group_join_timeout_secs = self.group_join_timeout_secs
        log_level: Union[Unset, str] = UNSET
        if not isinstance(self.log_level, Unset):
            log_level = self.log_level.value

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "topics": topics,
            }
        )
        if group_join_timeout_secs is not UNSET:
            field_dict["group_join_timeout_secs"] = group_join_timeout_secs
        if log_level is not UNSET:
            field_dict["log_level"] = log_level

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        topics = cast(List[str], d.pop("topics"))

        group_join_timeout_secs = d.pop("group_join_timeout_secs", UNSET)

        _log_level = d.pop("log_level", UNSET)
        log_level: Union[Unset, KafkaInputConfigLogLevel]
        if isinstance(_log_level, Unset):
            log_level = UNSET
        else:
            log_level = KafkaInputConfigLogLevel(_log_level)

        kafka_input_config = cls(
            topics=topics,
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
