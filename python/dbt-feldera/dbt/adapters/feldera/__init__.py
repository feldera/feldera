import os

from dbt.adapters.base import AdapterPlugin

from dbt.adapters.feldera.connections import FelderaConnectionManager as FelderaConnectionManager
from dbt.adapters.feldera.credentials import FelderaCredentials
from dbt.adapters.feldera.impl import FelderaAdapter

Plugin = AdapterPlugin(
    adapter=FelderaAdapter,
    credentials=FelderaCredentials,
    include_path=os.path.join(os.path.dirname(__file__), "..", "..", "include", "feldera"),
)
