import logging
from dataclasses import dataclass
from typing import Optional

from dbt.adapters.contracts.connection import Credentials

logger = logging.getLogger(__name__)


@dataclass
class FelderaCredentials(Credentials):
    """
    Connection credentials for the Feldera dbt adapter.

    Configured via ``profiles.yml``:

    .. code-block:: yaml

        my_project:
          target: dev
          outputs:
            dev:
              type: feldera
              host: "http://localhost:8080"
              api_key: "apikey:..."
              schema: "my_pipeline"
              compilation_profile: dev
              workers: 4
              timeout: 300
    """

    host: str = "http://localhost:8080"
    api_key: Optional[str] = None
    pipeline_name: Optional[str] = None
    compilation_profile: str = "dev"
    workers: int = 4
    timeout: int = 300

    @property
    def type(self) -> str:
        return "feldera"

    @property
    def unique_field(self) -> str:
        return self.host

    def _connection_keys(self):
        return (
            "host",
            "pipeline_name",
            "database",
            "schema",
            "compilation_profile",
            "workers",
        )
