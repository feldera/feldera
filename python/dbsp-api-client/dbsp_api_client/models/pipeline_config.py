from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

import attr

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.pipeline_config_inputs import PipelineConfigInputs
    from ..models.pipeline_config_outputs import PipelineConfigOutputs


T = TypeVar("T", bound="PipelineConfig")


@attr.s(auto_attribs=True)
class PipelineConfig:
    """Pipeline configuration specified by the user when creating
    a new pipeline instance.

        Attributes:
            inputs (PipelineConfigInputs): Input endpoint configuration.
            cpu_profiler (Union[Unset, bool]): Enable CPU profiler.
            max_buffering_delay_usecs (Union[Unset, int]): Maximal delay in microseconds to wait for
                `min_batch_size_records` to
                get buffered by the controller, defaults to 0.
            min_batch_size_records (Union[Unset, int]): Minimal input batch size.

                The controller delays pushing input records to the circuit until at
                least `min_batch_size_records` records have been received (total
                across all endpoints) or `max_buffering_delay_usecs` microseconds
                have passed since at least one input records has been buffered.
                Defaults to 0.
            workers (Union[Unset, int]): Number of DBSP worker threads.
            name (Union[Unset, None, str]): Pipeline name
            outputs (Union[Unset, PipelineConfigOutputs]): Output endpoint configuration.
    """

    inputs: "PipelineConfigInputs"
    cpu_profiler: Union[Unset, bool] = UNSET
    max_buffering_delay_usecs: Union[Unset, int] = UNSET
    min_batch_size_records: Union[Unset, int] = UNSET
    workers: Union[Unset, int] = UNSET
    name: Union[Unset, None, str] = UNSET
    outputs: Union[Unset, "PipelineConfigOutputs"] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        inputs = self.inputs.to_dict()

        cpu_profiler = self.cpu_profiler
        max_buffering_delay_usecs = self.max_buffering_delay_usecs
        min_batch_size_records = self.min_batch_size_records
        workers = self.workers
        name = self.name
        outputs: Union[Unset, Dict[str, Any]] = UNSET
        if not isinstance(self.outputs, Unset):
            outputs = self.outputs.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "inputs": inputs,
            }
        )
        if cpu_profiler is not UNSET:
            field_dict["cpu_profiler"] = cpu_profiler
        if max_buffering_delay_usecs is not UNSET:
            field_dict["max_buffering_delay_usecs"] = max_buffering_delay_usecs
        if min_batch_size_records is not UNSET:
            field_dict["min_batch_size_records"] = min_batch_size_records
        if workers is not UNSET:
            field_dict["workers"] = workers
        if name is not UNSET:
            field_dict["name"] = name
        if outputs is not UNSET:
            field_dict["outputs"] = outputs

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.pipeline_config_inputs import PipelineConfigInputs
        from ..models.pipeline_config_outputs import PipelineConfigOutputs

        d = src_dict.copy()
        inputs = PipelineConfigInputs.from_dict(d.pop("inputs"))

        cpu_profiler = d.pop("cpu_profiler", UNSET)

        max_buffering_delay_usecs = d.pop("max_buffering_delay_usecs", UNSET)

        min_batch_size_records = d.pop("min_batch_size_records", UNSET)

        workers = d.pop("workers", UNSET)

        name = d.pop("name", UNSET)

        _outputs = d.pop("outputs", UNSET)
        outputs: Union[Unset, PipelineConfigOutputs]
        if isinstance(_outputs, Unset):
            outputs = UNSET
        else:
            outputs = PipelineConfigOutputs.from_dict(_outputs)

        pipeline_config = cls(
            inputs=inputs,
            cpu_profiler=cpu_profiler,
            max_buffering_delay_usecs=max_buffering_delay_usecs,
            min_batch_size_records=min_batch_size_records,
            workers=workers,
            name=name,
            outputs=outputs,
        )

        pipeline_config.additional_properties = d
        return pipeline_config

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
