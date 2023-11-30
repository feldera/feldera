from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.kafka_config import KafkaConfig


T = TypeVar("T", bound="ServiceConfigType1")


@define
class ServiceConfigType1:
    """
    Attributes:
        kafka (KafkaConfig): Configuration for accessing a Kafka service.
    """

    kafka: "KafkaConfig"
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        kafka = self.kafka.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kafka": kafka,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.kafka_config import KafkaConfig

        d = src_dict.copy()
        kafka = KafkaConfig.from_dict(d.pop("kafka"))

        service_config_type_1 = cls(
            kafka=kafka,
        )

        service_config_type_1.additional_properties = d
        return service_config_type_1

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
