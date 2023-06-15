import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

import attr
from dateutil.parser import isoparse

from ..models.pipeline_status import PipelineStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.attached_connector import AttachedConnector


T = TypeVar("T", bound="PipelineDescr")


@attr.s(auto_attribs=True)
class PipelineDescr:
    """Pipeline descriptor.

    Attributes:
        attached_connectors (List['AttachedConnector']):
        config (str):
        description (str):
        name (str):
        pipeline_id (str): Unique pipeline id.
        port (int):
        status (PipelineStatus): Lifecycle of a pipeline.
        version (int): Version number.
        created (Union[Unset, None, datetime.datetime]):
        program_id (Union[Unset, None, str]): Unique program id.
    """

    attached_connectors: List["AttachedConnector"]
    config: str
    description: str
    name: str
    pipeline_id: str
    port: int
    status: PipelineStatus
    version: int
    created: Union[Unset, None, datetime.datetime] = UNSET
    program_id: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        attached_connectors = []
        for attached_connectors_item_data in self.attached_connectors:
            attached_connectors_item = attached_connectors_item_data.to_dict()

            attached_connectors.append(attached_connectors_item)

        config = self.config
        description = self.description
        name = self.name
        pipeline_id = self.pipeline_id
        port = self.port
        status = self.status.value

        version = self.version
        created: Union[Unset, None, str] = UNSET
        if not isinstance(self.created, Unset):
            created = self.created.isoformat() if self.created else None

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
                "port": port,
                "status": status,
                "version": version,
            }
        )
        if created is not UNSET:
            field_dict["created"] = created
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

        port = d.pop("port")

        status = PipelineStatus(d.pop("status"))

        version = d.pop("version")

        _created = d.pop("created", UNSET)
        created: Union[Unset, None, datetime.datetime]
        if _created is None:
            created = None
        elif isinstance(_created, Unset):
            created = UNSET
        else:
            created = isoparse(_created)

        program_id = d.pop("program_id", UNSET)

        pipeline_descr = cls(
            attached_connectors=attached_connectors,
            config=config,
            description=description,
            name=name,
            pipeline_id=pipeline_id,
            port=port,
            status=status,
            version=version,
            created=created,
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
