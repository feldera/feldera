from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RuntimeConfig")


@define
class RuntimeConfig:
    """Global pipeline configuration settings.

    Attributes:
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
    """

    cpu_profiler: Union[Unset, bool] = UNSET
    max_buffering_delay_usecs: Union[Unset, int] = UNSET
    min_batch_size_records: Union[Unset, int] = UNSET
    workers: Union[Unset, int] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        cpu_profiler = self.cpu_profiler
        max_buffering_delay_usecs = self.max_buffering_delay_usecs
        min_batch_size_records = self.min_batch_size_records
        workers = self.workers

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if cpu_profiler is not UNSET:
            field_dict["cpu_profiler"] = cpu_profiler
        if max_buffering_delay_usecs is not UNSET:
            field_dict["max_buffering_delay_usecs"] = max_buffering_delay_usecs
        if min_batch_size_records is not UNSET:
            field_dict["min_batch_size_records"] = min_batch_size_records
        if workers is not UNSET:
            field_dict["workers"] = workers

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        cpu_profiler = d.pop("cpu_profiler", UNSET)

        max_buffering_delay_usecs = d.pop("max_buffering_delay_usecs", UNSET)

        min_batch_size_records = d.pop("min_batch_size_records", UNSET)

        workers = d.pop("workers", UNSET)

        runtime_config = cls(
            cpu_profiler=cpu_profiler,
            max_buffering_delay_usecs=max_buffering_delay_usecs,
            min_batch_size_records=min_batch_size_records,
            workers=workers,
        )

        runtime_config.additional_properties = d
        return runtime_config

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
