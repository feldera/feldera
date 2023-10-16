from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..models.json_flavor import JsonFlavor
from ..models.json_update_format import JsonUpdateFormat
from ..types import UNSET, Unset

T = TypeVar("T", bound="JsonEncoderConfig")


@define
class JsonEncoderConfig:
    """
    Attributes:
        array (Union[Unset, bool]):
        buffer_size_records (Union[Unset, int]):
        json_flavor (Union[Unset, None, JsonFlavor]): Specifies JSON encoding used of table records.
        update_format (Union[Unset, JsonUpdateFormat]): Supported JSON data change event formats.

            Each element in a JSON-formatted input stream specifies
            an update to one or more records in an input table.  We support
            several different ways to represent such updates.
    """

    array: Union[Unset, bool] = UNSET
    buffer_size_records: Union[Unset, int] = UNSET
    json_flavor: Union[Unset, None, JsonFlavor] = UNSET
    update_format: Union[Unset, JsonUpdateFormat] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        array = self.array
        buffer_size_records = self.buffer_size_records
        json_flavor: Union[Unset, None, str] = UNSET
        if not isinstance(self.json_flavor, Unset):
            json_flavor = self.json_flavor.value if self.json_flavor else None

        update_format: Union[Unset, str] = UNSET
        if not isinstance(self.update_format, Unset):
            update_format = self.update_format.value

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if array is not UNSET:
            field_dict["array"] = array
        if buffer_size_records is not UNSET:
            field_dict["buffer_size_records"] = buffer_size_records
        if json_flavor is not UNSET:
            field_dict["json_flavor"] = json_flavor
        if update_format is not UNSET:
            field_dict["update_format"] = update_format

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        array = d.pop("array", UNSET)

        buffer_size_records = d.pop("buffer_size_records", UNSET)

        _json_flavor = d.pop("json_flavor", UNSET)
        json_flavor: Union[Unset, None, JsonFlavor]
        if _json_flavor is None:
            json_flavor = None
        elif isinstance(_json_flavor, Unset):
            json_flavor = UNSET
        else:
            json_flavor = JsonFlavor(_json_flavor)

        _update_format = d.pop("update_format", UNSET)
        update_format: Union[Unset, JsonUpdateFormat]
        if isinstance(_update_format, Unset):
            update_format = UNSET
        else:
            update_format = JsonUpdateFormat(_update_format)

        json_encoder_config = cls(
            array=array,
            buffer_size_records=buffer_size_records,
            json_flavor=json_flavor,
            update_format=update_format,
        )

        json_encoder_config.additional_properties = d
        return json_encoder_config

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
