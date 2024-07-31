from typing import Mapping, Any, Optional
from feldera.rest.sql_table import SQLTable
from feldera.rest.sql_view import SQLView


class Pipeline:
    """
    Represents a Feldera pipeline
    """

    def __init__(
        self,
        name: str,
        sql: str,
        program_config: Mapping[str, Any],
        runtime_config: Mapping[str, Any],
        description: Optional[str] = None,
    ):
        """
        Initializes a new pipeline

        :param name: The name of the pipeline
        :param sql: The SQL code of the pipeline
        :param program_config: The program config of the pipeline
        :param runtime_config: The configuration of the pipeline
        :param description: Optional. The description of the pipeline
        """

        self.name: str = name
        self.program_code: str = sql.strip()
        self.description: Optional[str] = description
        self.program_config: Mapping[str, Any] = program_config
        self.runtime_config: Mapping[str, Any] = runtime_config
        self.id: Optional[str] = id
        self.state: Optional[dict] = None
        self.tables: list[SQLTable] = []
        self.views: list[SQLView] = []

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        pipeline = cls("", "", {}, {})
        pipeline.__dict__ = d
        pipeline.tables = []
        pipeline.views = []

        info = d.get("program_info")

        if info is not None:
            for i in info['schema']['inputs']:
                tbl = SQLTable.from_dict(i)
                pipeline.tables.append(tbl)

            for output in info['schema']['outputs']:
                v = SQLView.from_dict(output)
                pipeline.views.append(v)

        return pipeline
