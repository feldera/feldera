from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

T = TypeVar("T", bound="JsonEncoderConfig")


@define
class JsonEncoderConfig:
    """
    Attributes:
        array (Union[Unset, bool]):
        buffer_size_records (Union[Unset, int]):
    """

    array: Union[Unset, bool] = UNSET
    buffer_size_records: Union[Unset, int] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        array = self.array
        buffer_size_records = self.buffer_size_records

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if array is not UNSET:
            field_dict["array"] = array
        if buffer_size_records is not UNSET:
            field_dict["buffer_size_records"] = buffer_size_records

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        array = d.pop("array", UNSET)

        buffer_size_records = d.pop("buffer_size_records", UNSET)

        json_encoder_config = cls(
            array=array,
            buffer_size_records=buffer_size_records,
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
