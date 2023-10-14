from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

T = TypeVar("T", bound="FileInputConfig")


@define
class FileInputConfig:
    """Configuration for reading data from a file with `FileInputTransport`

    Attributes:
        path (str): File path.
        buffer_size_bytes (Union[Unset, None, int]): Read buffer size.

            Default: when this parameter is not specified, a platform-specific
            default is used.
        follow (Union[Unset, bool]): Enable file following.

            When `false`, the endpoint outputs an `InputConsumer::eoi`
            message and stops upon reaching the end of file.  When `true`, the
            endpoint will keep watching the file and outputting any new content
            appended to it.
    """

    path: str
    buffer_size_bytes: Union[Unset, None, int] = UNSET
    follow: Union[Unset, bool] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        path = self.path
        buffer_size_bytes = self.buffer_size_bytes
        follow = self.follow

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "path": path,
            }
        )
        if buffer_size_bytes is not UNSET:
            field_dict["buffer_size_bytes"] = buffer_size_bytes
        if follow is not UNSET:
            field_dict["follow"] = follow

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        path = d.pop("path")

        buffer_size_bytes = d.pop("buffer_size_bytes", UNSET)

        follow = d.pop("follow", UNSET)

        file_input_config = cls(
            path=path,
            buffer_size_bytes=buffer_size_bytes,
            follow=follow,
        )

        file_input_config.additional_properties = d
        return file_input_config

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
