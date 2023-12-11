from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.attached_connector import AttachedConnector
    from ..models.runtime_config import RuntimeConfig


T = TypeVar("T", bound="PipelineDescr")


@define
class PipelineDescr:
    """Pipeline descriptor.

    Attributes:
        attached_connectors (List['AttachedConnector']):
        config (RuntimeConfig): Global pipeline configuration settings. This is the publicly
            exposed type for users to configure pipelines.
        description (str):
        name (str):
        pipeline_id (str): Unique pipeline id.
        version (int): Version number.
        program_name (Union[Unset, None, str]):
    """

    attached_connectors: List["AttachedConnector"]
    config: "RuntimeConfig"
    description: str
    name: str
    pipeline_id: str
    version: int
    program_name: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        attached_connectors = []
        for attached_connectors_item_data in self.attached_connectors:
            attached_connectors_item = attached_connectors_item_data.to_dict()

            attached_connectors.append(attached_connectors_item)

        config = self.config.to_dict()

        description = self.description
        name = self.name
        pipeline_id = self.pipeline_id
        version = self.version
        program_name = self.program_name

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "attached_connectors": attached_connectors,
                "config": config,
                "description": description,
                "name": name,
                "pipeline_id": pipeline_id,
                "version": version,
            }
        )
        if program_name is not UNSET:
            field_dict["program_name"] = program_name

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.attached_connector import AttachedConnector
        from ..models.runtime_config import RuntimeConfig

        d = src_dict.copy()
        attached_connectors = []
        _attached_connectors = d.pop("attached_connectors")
        for attached_connectors_item_data in _attached_connectors:
            attached_connectors_item = AttachedConnector.from_dict(attached_connectors_item_data)

            attached_connectors.append(attached_connectors_item)

        config = RuntimeConfig.from_dict(d.pop("config"))

        description = d.pop("description")

        name = d.pop("name")

        pipeline_id = d.pop("pipeline_id")

        version = d.pop("version")

        program_name = d.pop("program_name", UNSET)

        pipeline_descr = cls(
            attached_connectors=attached_connectors,
            config=config,
            description=description,
            name=name,
            pipeline_id=pipeline_id,
            version=version,
            program_name=program_name,
        )

        pipeline_descr.additional_properties = d
        return pipeline_descr

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
