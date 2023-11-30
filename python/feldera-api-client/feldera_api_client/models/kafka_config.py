from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, cast

from attrs import define, field

if TYPE_CHECKING:
    from ..models.kafka_config_options import KafkaConfigOptions


T = TypeVar("T", bound="KafkaConfig")


@define
class KafkaConfig:
    """Configuration for accessing a Kafka service.

    Attributes:
        bootstrap_servers (List[str]): List of bootstrap servers
        options (KafkaConfigOptions): Additional Kafka options
    """

    bootstrap_servers: List[str]
    options: "KafkaConfigOptions"
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        bootstrap_servers = self.bootstrap_servers

        options = self.options.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "bootstrap_servers": bootstrap_servers,
                "options": options,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.kafka_config_options import KafkaConfigOptions

        d = src_dict.copy()
        bootstrap_servers = cast(List[str], d.pop("bootstrap_servers"))

        options = KafkaConfigOptions.from_dict(d.pop("options"))

        kafka_config = cls(
            bootstrap_servers=bootstrap_servers,
            options=options,
        )

        kafka_config.additional_properties = d
        return kafka_config

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
