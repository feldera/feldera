from feldera.rest.feldera_client import FelderaClient as FelderaClient
from feldera.pipeline import Pipeline as Pipeline
from feldera.pipeline_builder import PipelineBuilder as PipelineBuilder
from feldera.rest._helpers import determine_client_version

__version__ = determine_client_version()

import pretty_errors

pretty_errors.configure(
    line_number_first=True,
)

pretty_errors.activate()
