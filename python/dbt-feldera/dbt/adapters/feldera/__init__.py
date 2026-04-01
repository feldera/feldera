import os

from dbt.adapters.base import AdapterPlugin

from dbt.adapters.feldera.__version__ import version as version  # noqa: PLC0414 — re-exported as public API

__version__ = version
from dbt.adapters.feldera.connections import FelderaConnectionManager as FelderaConnectionManager
from dbt.adapters.feldera.credentials import FelderaCredentials
from dbt.adapters.feldera.impl import FelderaAdapter

Plugin = AdapterPlugin(
    adapter=FelderaAdapter,
    credentials=FelderaCredentials,
    include_path=os.path.join(os.path.dirname(__file__), "..", "..", "include", "feldera"),
)
