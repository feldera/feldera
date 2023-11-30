from typing import Any, Dict, List, Type, TypeVar

from attrs import define, field

T = TypeVar("T", bound="MysqlConfig")


@define
class MysqlConfig:
    """Configuration for accessing a MySQL database service.

    Attributes:
        hostname (str):
        password (str):
        port (str):
        user (str):
    """

    hostname: str
    password: str
    port: str
    user: str
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        hostname = self.hostname
        password = self.password
        port = self.port
        user = self.user

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "hostname": hostname,
                "password": password,
                "port": port,
                "user": user,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        hostname = d.pop("hostname")

        password = d.pop("password")

        port = d.pop("port")

        user = d.pop("user")

        mysql_config = cls(
            hostname=hostname,
            password=password,
            port=port,
            user=user,
        )

        mysql_config.additional_properties = d
        return mysql_config

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
