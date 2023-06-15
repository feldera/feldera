from typing import TYPE_CHECKING, Any, Dict, List, Type, TypeVar, Union

import attr

from ..models.program_status_type_0 import ProgramStatusType0
from ..models.program_status_type_1 import ProgramStatusType1
from ..models.program_status_type_2 import ProgramStatusType2
from ..models.program_status_type_3 import ProgramStatusType3
from ..models.program_status_type_4 import ProgramStatusType4
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.program_status_type_5 import ProgramStatusType5
    from ..models.program_status_type_6 import ProgramStatusType6
    from ..models.program_status_type_7 import ProgramStatusType7


T = TypeVar("T", bound="ProgramDescr")


@attr.s(auto_attribs=True)
class ProgramDescr:
    """Program descriptor.

    Attributes:
        description (str): Program description.
        name (str): Program name (doesn't have to be unique).
        program_id (str): Unique program id.
        status (Union['ProgramStatusType5', 'ProgramStatusType6', 'ProgramStatusType7', ProgramStatusType0,
            ProgramStatusType1, ProgramStatusType2, ProgramStatusType3, ProgramStatusType4]): Program compilation status.
        version (int): Version number.
        schema (Union[Unset, None, str]): A JSON description of the SQL tables and view declarations including
            field names and types.

            The schema is set/updated whenever the `status` field reaches >=
            `ProgramStatus::CompilingRust`.

            # Example

            The given SQL program:

            ```no_run
            CREATE TABLE USERS ( name varchar );
            CREATE VIEW OUTPUT_USERS as SELECT * FROM USERS;
            ```

            Would lead the following JSON string in `schema`:

            ```no_run
            {
            "inputs": [{
            "name": "USERS",
            "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
            }],
            "outputs": [{
            "name": "OUTPUT_USERS",
            "fields": [{ "name": "NAME", "type": "VARCHAR", "nullable": true }]
            }]
            }
            ```
    """

    description: str
    name: str
    program_id: str
    status: Union[
        "ProgramStatusType5",
        "ProgramStatusType6",
        "ProgramStatusType7",
        ProgramStatusType0,
        ProgramStatusType1,
        ProgramStatusType2,
        ProgramStatusType3,
        ProgramStatusType4,
    ]
    version: int
    schema: Union[Unset, None, str] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        from ..models.program_status_type_5 import ProgramStatusType5
        from ..models.program_status_type_6 import ProgramStatusType6

        description = self.description
        name = self.name
        program_id = self.program_id
        status: Union[Dict[str, Any], str]

        if isinstance(self.status, ProgramStatusType0):
            status = self.status.value

        elif isinstance(self.status, ProgramStatusType1):
            status = self.status.value

        elif isinstance(self.status, ProgramStatusType2):
            status = self.status.value

        elif isinstance(self.status, ProgramStatusType3):
            status = self.status.value

        elif isinstance(self.status, ProgramStatusType4):
            status = self.status.value

        elif isinstance(self.status, ProgramStatusType5):
            status = self.status.to_dict()

        elif isinstance(self.status, ProgramStatusType6):
            status = self.status.to_dict()

        else:
            status = self.status.to_dict()

        version = self.version
        schema = self.schema

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "description": description,
                "name": name,
                "program_id": program_id,
                "status": status,
                "version": version,
            }
        )
        if schema is not UNSET:
            field_dict["schema"] = schema

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        from ..models.program_status_type_5 import ProgramStatusType5
        from ..models.program_status_type_6 import ProgramStatusType6
        from ..models.program_status_type_7 import ProgramStatusType7

        d = src_dict.copy()
        description = d.pop("description")

        name = d.pop("name")

        program_id = d.pop("program_id")

        def _parse_status(
            data: object,
        ) -> Union[
            "ProgramStatusType5",
            "ProgramStatusType6",
            "ProgramStatusType7",
            ProgramStatusType0,
            ProgramStatusType1,
            ProgramStatusType2,
            ProgramStatusType3,
            ProgramStatusType4,
        ]:
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_program_status_type_0 = ProgramStatusType0(data)

                return componentsschemas_program_status_type_0
            except:  # noqa: E722
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_program_status_type_1 = ProgramStatusType1(data)

                return componentsschemas_program_status_type_1
            except:  # noqa: E722
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_program_status_type_2 = ProgramStatusType2(data)

                return componentsschemas_program_status_type_2
            except:  # noqa: E722
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_program_status_type_3 = ProgramStatusType3(data)

                return componentsschemas_program_status_type_3
            except:  # noqa: E722
                pass
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_program_status_type_4 = ProgramStatusType4(data)

                return componentsschemas_program_status_type_4
            except:  # noqa: E722
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_program_status_type_5 = ProgramStatusType5.from_dict(data)

                return componentsschemas_program_status_type_5
            except:  # noqa: E722
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_program_status_type_6 = ProgramStatusType6.from_dict(data)

                return componentsschemas_program_status_type_6
            except:  # noqa: E722
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_program_status_type_7 = ProgramStatusType7.from_dict(data)

            return componentsschemas_program_status_type_7

        status = _parse_status(d.pop("status"))

        version = d.pop("version")

        schema = d.pop("schema", UNSET)

        program_descr = cls(
            description=description,
            name=name,
            program_id=program_id,
            status=status,
            version=version,
            schema=schema,
        )

        program_descr.additional_properties = d
        return program_descr

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
