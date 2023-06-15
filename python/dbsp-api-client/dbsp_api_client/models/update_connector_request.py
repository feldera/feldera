from typing import Any, Dict, List, Type, TypeVar, Union

import attr

from ..types import UNSET, Unset

T = TypeVar("T", bound="UpdateConnectorRequest")


@attr.s(auto_attribs=True)
class UpdateConnectorRequest:
    """Request to update an existing data-connector.

    Attributes:
        connector_id (str): Unique connector id.
        description (str): New connector description.
        name (str): New connector name.
        config (Union[Unset, None, str]): New config YAML. If absent, existing YAML will be kept unmodified.
    """

    connector_id: str
    description: str
    name: str
    config: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        connector_id = self.connector_id
        description = self.description
        name = self.name
        config = self.config

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "connector_id": connector_id,
                "description": description,
                "name": name,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        connector_id = d.pop("connector_id")

        description = d.pop("description")

        name = d.pop("name")

        config = d.pop("config", UNSET)

        update_connector_request = cls(
            connector_id=connector_id,
            description=description,
            name=name,
            config=config,
        )

        update_connector_request.additional_properties = d
        return update_connector_request

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
