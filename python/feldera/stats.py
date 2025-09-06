from typing import Mapping, Any, Optional, List
from feldera.enums import PipelineStatus, TransactionStatus
from datetime import datetime
import uuid


class PipelineStatistics:
    """
    Represents statistics reported by a pipeline's "/stats" endpoint.
    """

    def __init__(self):
        """
        Initializes as an empty set of statistics.
        """

        self.global_metrics: GlobalPipelineMetrics = GlobalPipelineMetrics()
        self.suspend_error: Optional[Any] = None
        self.inputs: Mapping[List[InputEndpointStatus]] = {}
        self.outputs: Mapping[List[OutputEndpointStatus]] = {}

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        pipeline = cls()
        pipeline.global_metrics = GlobalPipelineMetrics.from_dict(d["global_metrics"])
        pipeline.inputs = [
            InputEndpointStatus.from_dict(input) for input in d["inputs"]
        ]
        pipeline.outputs = [
            OutputEndpointStatus.from_dict(output) for output in d["outputs"]
        ]
        return pipeline


class GlobalPipelineMetrics:
    """Represents the "global_metrics" object within the pipeline's
    "/stats" endpoint reply.
    """

    def __init__(self):
        """
        Initializes as an empty set of metrics.
        """
        self.state: Optional[PipelineStatus] = None
        self.bootstrap_in_progress: Optional[bool] = None
        self.rss_bytes: Optional[int] = None
        self.cpu_msecs: Optional[int] = None
        self.start_time: Optional[datetime] = None
        self.incarnation_uuid: Optional[uuid.UUID] = None
        self.storage_bytes: Optional[int] = None
        self.storage_mb_secs: Optional[int] = None
        self.runtime_elapsed_msecs: Optional[int] = None
        self.buffered_input_records: Optional[int] = None
        self.total_input_records: Optional[int] = None
        self.total_processed_records: Optional[int] = None
        self.total_completed_records: Optional[int] = None
        self.pipeline_complete: Optional[bool] = None
        self.transaction_status: Optional[TransactionStatus] = None
        self.transaction_id: Optional[int] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        metrics = cls()
        metrics.__dict__.update(d)
        metrics.state = PipelineStatus.from_str(d["state"])
        metrics.incarnation_uuid = uuid.UUID(d["incarnation_uuid"])
        metrics.start_time = datetime.fromtimestamp(d["start_time"])
        metrics.transaction_status = TransactionStatus.from_str(d["transaction_status"])
        return metrics


class InputEndpointStatus:
    """Represents one member of the "inputs" array within the
    pipeline's "/stats" endpoint reply.
    """

    def __init__(self):
        """Initializes an empty status."""
        self.endpoint_name: Optional[str] = None
        self.config: Optional[Mapping] = None
        self.metrics: Optional[InputEndpointMetrics] = None
        self.fatal_error: Optional[str] = None
        self.paused: Optional[bool] = None
        self.barrier: Optional[bool] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.__dict__.update(d)
        status.metrics = InputEndpointMetrics.from_dict(d["metrics"])
        return status


class InputEndpointMetrics:
    """Represents the "metrics" member within an input endpoint status
    in the pipeline's "/stats" endpoint reply.
    """

    def __init__(self):
        self.total_bytes: Optional[int] = None
        self.total_records: Optional[int] = None
        self.buffered_records: Optional[int] = None
        self.num_transport_errors: Optional[int] = None
        self.num_parse_errors: Optional[int] = None
        self.end_of_input: Optional[bool] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        metrics = cls()
        metrics.__dict__.update(d)
        return metrics


class OutputEndpointStatus:
    """Represents one member of the "outputs" array within the
    pipeline's "/stats" endpoint reply.
    """

    def __init__(self):
        """Initializes an empty status."""
        self.endpoint_name: Optional[str] = None
        self.config: Optional[Mapping] = None
        self.metrics: Optional[OutputEndpointMetrics] = None
        self.fatal_error: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.__dict__.update(d)
        status.metrics = OutputEndpointMetrics.from_dict(d["metrics"])
        return status


class OutputEndpointMetrics:
    """Represents the "metrics" member within an output endpoint status
    in the pipeline's "/stats" endpoint reply.
    """

    def __init__(self):
        self.transmitted_records: Optional[int] = None
        self.transmitted_bytes: Optional[int] = None
        self.queued_records: Optional[int] = None
        self.queued_batches: Optional[int] = None
        self.num_encode_errors: Optional[int] = None
        self.num_transport_errors: Optional[int] = None
        self.total_processed_input_records: Optional[int] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        metrics = cls()
        metrics.__dict__.update(d)
        return metrics
