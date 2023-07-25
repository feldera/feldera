from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

from attrs import define, field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.attached_connector import AttachedConnector


T = TypeVar("T", bound="PipelineDescr")


@define
class PipelineDescr:
    """Pipeline descriptor.

    Attributes:
        attached_connectors (List['AttachedConnector']):
        config (str):
        description (str):
        name (str):
        pipeline_id (str): Unique pipeline id.
        version (int): Version number.
        program_id (Union[Unset, None, str]): Unique program id.
    """

    attached_connectors: List["AttachedConnector"]
    config: str
    description: str
    name: str
    pipeline_id: str
    version: int
    program_id: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        attached_connectors = []
        for attached_connectors_item_data in self.attached_connectors:
            attached_connectors_item = attached_connectors_item_data.to_dict()

            attached_connectors.append(attached_connectors_item)

        config = self.config
        description = self.description
        name = self.name
        pipeline_id = self.pipeline_id
        version = self.version
        program_id = self.program_id

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
        if program_id is not UNSET:
            field_dict["program_id"] = program_id

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.attached_connector import AttachedConnector

        d = src_dict.copy()
        attached_connectors = []
        _attached_connectors = d.pop("attached_connectors")
        for attached_connectors_item_data in _attached_connectors:
            attached_connectors_item = AttachedConnector.from_dict(attached_connectors_item_data)

            attached_connectors.append(attached_connectors_item)

        config = d.pop("config")

        description = d.pop("description")

        name = d.pop("name")

        pipeline_id = d.pop("pipeline_id")

        version = d.pop("version")

        program_id = d.pop("program_id", UNSET)

        pipeline_descr = cls(
            attached_connectors=attached_connectors,
            config=config,
            description=description,
            name=name,
            pipeline_id=pipeline_id,
            version=version,
            program_id=program_id,
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
