from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

from attrs import define, field

if TYPE_CHECKING:
    from ..models.kafka_service import KafkaService


T = TypeVar("T", bound="ServiceConfigType0")


@define
class ServiceConfigType0:
    """
    Attributes:
        kafka (KafkaService): Configuration for accessing a Kafka service.
    """

    kafka: "KafkaService"
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
        from ..models.kafka_service import KafkaService

        d = src_dict.copy()
        kafka = KafkaService.from_dict(d.pop("kafka"))

        service_config_type_0 = cls(
            kafka=kafka,
        )

        service_config_type_0.additional_properties = d
        return service_config_type_0

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
