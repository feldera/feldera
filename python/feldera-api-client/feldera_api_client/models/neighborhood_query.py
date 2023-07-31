from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.neighborhood_query_anchor import NeighborhoodQueryAnchor


T = TypeVar("T", bound="NeighborhoodQuery")


@define
class NeighborhoodQuery:
    """A request to output a specific neighborhood of a table or view.
    The neighborhood is defined in terms of its central point (`anchor`)
    and the number of rows preceding and following the anchor to output.

        Attributes:
            after (int):
            before (int):
            anchor (Union[Unset, None, NeighborhoodQueryAnchor]):
    """

    after: int
    before: int
    anchor: Union[Unset, None, "NeighborhoodQueryAnchor"] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        after = self.after
        before = self.before
        anchor: Union[Unset, None, Dict[str, Any]] = UNSET
        if not isinstance(self.anchor, Unset):
            anchor = self.anchor.to_dict() if self.anchor else None

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "after": after,
                "before": before,
            }
        )
        if anchor is not UNSET:
            field_dict["anchor"] = anchor

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.neighborhood_query_anchor import NeighborhoodQueryAnchor

        d = src_dict.copy()
        after = d.pop("after")

        before = d.pop("before")

        _anchor = d.pop("anchor", UNSET)
        anchor: Union[Unset, None, NeighborhoodQueryAnchor]
        if _anchor is None:
            anchor = None
        elif isinstance(_anchor, Unset):
            anchor = UNSET
        else:
            anchor = NeighborhoodQueryAnchor.from_dict(_anchor)

        neighborhood_query = cls(
            after=after,
            before=before,
            anchor=anchor,
        )

        neighborhood_query.additional_properties = d
        return neighborhood_query

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
