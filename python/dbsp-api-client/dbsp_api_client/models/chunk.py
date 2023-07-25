from io import BytesIO
from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, File, FileJsonType, Unset

if TYPE_CHECKING:
    from ..models.chunk_json_data import ChunkJsonData


T = TypeVar("T", bound="Chunk")


@define
class Chunk:
    """A set of updates to a SQL table or view.

    The `sequence_number` field stores the offset of the chunk relative to the
    start of the stream and can be used to implement reliable delivery.
    The payload is stored in the `bin_data`, `text_data`, or `json_data` field
    depending on the data format used.

        Attributes:
            sequence_number (int):
            bin_data (Union[Unset, None, File]): Base64 encoded binary payload, e.g., bincode.
            json_data (Union[Unset, None, ChunkJsonData]): JSON payload.
            text_data (Union[Unset, None, str]): Text payload, e.g., CSV.
    """

    sequence_number: int
    bin_data: Union[Unset, None, File] = UNSET
    json_data: Union[Unset, None, "ChunkJsonData"] = UNSET
    text_data: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        sequence_number = self.sequence_number
        bin_data: Union[Unset, None, FileJsonType] = UNSET
        if not isinstance(self.bin_data, Unset):
            bin_data = self.bin_data.to_tuple() if self.bin_data else None

        json_data: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.json_data, Unset):
            json_data = self.json_data.to_dict() if self.json_data else None

        text_data = self.text_data

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "sequence_number": sequence_number,
            }
        )
        if bin_data is not UNSET:
            field_dict["bin_data"] = bin_data
        if json_data is not UNSET:
            field_dict["json_data"] = json_data
        if text_data is not UNSET:
            field_dict["text_data"] = text_data

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.chunk_json_data import ChunkJsonData

        d = src_dict.copy()
        sequence_number = d.pop("sequence_number")

        _bin_data = d.pop("bin_data", UNSET)
        bin_data: Union[Unset, None, File]
        if _bin_data is None:
            bin_data = None
        elif isinstance(_bin_data, Unset):
            bin_data = UNSET
        else:
            bin_data = File(payload=BytesIO(_bin_data))

        _json_data = d.pop("json_data", UNSET)
        json_data: Union[Unset, None, ChunkJsonData]
        if _json_data is None:
            json_data = None
        elif isinstance(_json_data, Unset):
            json_data = UNSET
        else:
            json_data = ChunkJsonData.from_dict(_json_data)

        text_data = d.pop("text_data", UNSET)

        chunk = cls(
            sequence_number=sequence_number,
            bin_data=bin_data,
            json_data=json_data,
            text_data=text_data,
        )

        chunk.additional_properties = d
        return chunk

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
