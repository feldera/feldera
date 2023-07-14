from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar

import attr

if TYPE_CHECKING:
    from ..models.pipeline_descr import PipelineDescr
    from ..models.pipeline_runtime_state import PipelineRuntimeState


T = TypeVar("T", bound="Pipeline")


@attr.s(auto_attribs=True)
class Pipeline:
    """State of a pipeline, including static configuration
    and runtime status.

        Attributes:
            descriptor (PipelineDescr): Pipeline descriptor.
            state (PipelineRuntimeState): Runtime state of the pipeine.
    """

    descriptor: "PipelineDescr"
    state: "PipelineRuntimeState"
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        descriptor = self.descriptor.to_dict()

        state = self.state.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "descriptor": descriptor,
                "state": state,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.pipeline_descr import PipelineDescr
        from ..models.pipeline_runtime_state import PipelineRuntimeState

        d = src_dict.copy()
        descriptor = PipelineDescr.from_dict(d.pop("descriptor"))

        state = PipelineRuntimeState.from_dict(d.pop("state"))

        pipeline = cls(
            descriptor=descriptor,
            state=state,
        )

        pipeline.additional_properties = d
        return pipeline

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
