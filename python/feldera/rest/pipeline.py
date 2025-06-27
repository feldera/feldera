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
        udf_rust: str,
        udf_toml: str,
        program_config: Mapping[str, Any],
        runtime_config: Mapping[str, Any],
        description: Optional[str] = None,
    ):
        """
        Initializes a new pipeline

        :param name: The name of the pipeline
        :param sql: The SQL code of the pipeline
        :param udf_rust: Rust code for UDFs
        :param udf_toml: Rust dependencies required by UDFs (in the TOML format)
        :param program_config: The program config of the pipeline
        :param runtime_config: The configuration of the pipeline
        :param description: Optional. The description of the pipeline
        """

        self.name: str = name
        self.program_code: str = sql.strip()
        self.udf_rust: str = udf_rust
        self.udf_toml: str = udf_toml
        self.description: Optional[str] = description
        self.program_config: Mapping[str, Any] = program_config
        self.runtime_config: Mapping[str, Any] = runtime_config
        self.id: Optional[str] = None
        self.tables: list[SQLTable] = []
        self.views: list[SQLView] = []
        self.deployment_status: Optional[str] = None
        self.deployment_status_since: Optional[str] = None
        self.created_at: Optional[str] = None
        self.version: Optional[int] = None
        self.program_version: Optional[int] = None
        self.deployment_config: Optional[dict] = None
        self.deployment_desired_status: Optional[str] = None
        self.deployment_error: Optional[dict] = None
        self.deployment_location: Optional[str] = None
        self.program_binary_url: Optional[str] = None
        self.program_info: Optional[dict] = (
            None  # info about input & output connectors and the schema
        )
        self.program_status: Optional[str] = None
        self.program_status_since: Optional[str] = None
        self.program_error: Optional[dict] = None
        self.storage_status: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        pipeline = cls("", "", "", "", {}, {})
        pipeline.__dict__ = d
        pipeline.tables = []
        pipeline.views = []

        info = d.get("program_info")

        if info is not None:
            for i in info["schema"]["inputs"]:
                tbl = SQLTable.from_dict(i)
                pipeline.tables.append(tbl)

            for output in info["schema"]["outputs"]:
                v = SQLView.from_dict(output)
                pipeline.views.append(v)

        return pipeline
