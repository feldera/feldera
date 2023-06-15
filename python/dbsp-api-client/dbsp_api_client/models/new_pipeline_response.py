from typing import Any, Dict, List, Type, TypeVar

import attr

T = TypeVar("T", bound="NewPipelineResponse")


@attr.s(auto_attribs=True)
class NewPipelineResponse:
    """Response to a config creation request.

    Attributes:
        pipeline_id (str): Unique pipeline id.
        version (int): Version number.
    """

    pipeline_id: str
    version: int
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        pipeline_id = self.pipeline_id
        version = self.version

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "pipeline_id": pipeline_id,
                "version": version,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        pipeline_id = d.pop("pipeline_id")

        version = d.pop("version")

        new_pipeline_response = cls(
            pipeline_id=pipeline_id,
            version=version,
        )

        new_pipeline_response.additional_properties = d
        return new_pipeline_response

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
