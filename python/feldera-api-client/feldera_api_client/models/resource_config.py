from typing import Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResourceConfig")


@define
class ResourceConfig:
    """
    Attributes:
        cpu_cores_max (Union[Unset, None, int]): The maximum number of CPU cores to reserve
            for an instance of this pipeline
        cpu_cores_min (Union[Unset, None, int]): The minimum number of CPU cores to reserve
            for an instance of this pipeline
        memory_mb_max (Union[Unset, None, int]): The maximum memory in Megabytes to reserve
            for an instance of this pipeline
        memory_mb_min (Union[Unset, None, int]): The minimum memory in Megabytes to reserve
            for an instance of this pipeline
        storage_mb_max (Union[Unset, None, int]): The total storage in Megabytes to reserve
            for an instance of this pipeline
    """

    cpu_cores_max: Union[Unset, None, int] = UNSET
    cpu_cores_min: Union[Unset, None, int] = UNSET
    memory_mb_max: Union[Unset, None, int] = UNSET
    memory_mb_min: Union[Unset, None, int] = UNSET
    storage_mb_max: Union[Unset, None, int] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        cpu_cores_max = self.cpu_cores_max
        cpu_cores_min = self.cpu_cores_min
        memory_mb_max = self.memory_mb_max
        memory_mb_min = self.memory_mb_min
        storage_mb_max = self.storage_mb_max

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if cpu_cores_max is not UNSET:
            field_dict["cpu_cores_max"] = cpu_cores_max
        if cpu_cores_min is not UNSET:
            field_dict["cpu_cores_min"] = cpu_cores_min
        if memory_mb_max is not UNSET:
            field_dict["memory_mb_max"] = memory_mb_max
        if memory_mb_min is not UNSET:
            field_dict["memory_mb_min"] = memory_mb_min
        if storage_mb_max is not UNSET:
            field_dict["storage_mb_max"] = storage_mb_max

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        cpu_cores_max = d.pop("cpu_cores_max", UNSET)

        cpu_cores_min = d.pop("cpu_cores_min", UNSET)

        memory_mb_max = d.pop("memory_mb_max", UNSET)

        memory_mb_min = d.pop("memory_mb_min", UNSET)

        storage_mb_max = d.pop("storage_mb_max", UNSET)

        resource_config = cls(
            cpu_cores_max=cpu_cores_max,
            cpu_cores_min=cpu_cores_min,
            memory_mb_max=memory_mb_max,
            memory_mb_min=memory_mb_min,
            storage_mb_max=storage_mb_max,
        )

        resource_config.additional_properties = d
        return resource_config

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
