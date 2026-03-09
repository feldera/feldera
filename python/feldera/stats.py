from typing import Mapping, Any, Optional, List
from feldera._helpers import (
    expect_bool,
    expect_int,
    expect_mapping,
    expect_str,
    parse_datetime,
)
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
        self.transaction_status: Optional[TransactionStatus] = None
        self.transaction_id: Optional[int] = None
        self.commit_progress: Optional[CommitProgressSummary] = None
        self.transaction_initiators: Optional[TransactionInitiators] = None
        self.rss_bytes: Optional[int] = None
        self.cpu_msecs: Optional[int] = None
        self.uptime_msecs: Optional[float] = None
        self.start_time: Optional[datetime] = None
        self.incarnation_uuid: Optional[uuid.UUID] = None
        self.initial_start_time: Optional[datetime] = None
        self.storage_bytes: Optional[int] = None
        self.storage_mb_secs: Optional[int] = None
        self.runtime_elapsed_msecs: Optional[int] = None
        self.buffered_input_records: Optional[int] = None
        self.total_input_records: Optional[int] = None
        self.buffered_input_bytes: Optional[int] = None
        self.total_input_bytes: Optional[int] = None
        self.total_processed_records: Optional[int] = None
        self.total_processed_bytes: Optional[int] = None
        self.total_completed_records: Optional[int] = None
        self.output_stall_msecs: Optional[int] = None
        self.total_initiated_steps: Optional[int] = None
        self.total_completed_steps: Optional[int] = None
        self.pipeline_complete: Optional[bool] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        metrics = cls()
        metrics.__dict__.update(d)
        metrics.state = PipelineStatus.from_str(d["state"])
        metrics.incarnation_uuid = uuid.UUID(d["incarnation_uuid"])
        metrics.start_time = datetime.fromtimestamp(d["start_time"])
        metrics.initial_start_time = datetime.fromtimestamp(d["start_time"])
        metrics.transaction_status = TransactionStatus.from_str(d["transaction_status"])
        return metrics


class ConnectorError:
    """Represents a connector error item reported by connector status endpoints."""

    def __init__(self):
        self.timestamp: datetime = datetime.fromtimestamp(0)
        self.index: int = 0
        self.tag: Optional[str] = None
        self.message: str = ""

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        error = cls()
        error.timestamp = parse_datetime(expect_str(d, "timestamp"), "timestamp")
        error.index = expect_int(d, "index")
        tag = d.get("tag")
        if tag is not None and not isinstance(tag, str):
            raise ValueError("invalid optional field 'tag': expected string or null")
        error.tag = tag
        error.message = expect_str(d, "message")
        return error


class ConnectorHealth:
    """Health status reported for a connector."""

    HEALTHY = "Healthy"
    UNHEALTHY = "Unhealthy"

    def __init__(self):
        self.status: str = ConnectorHealth.HEALTHY
        self.description: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        health = cls()
        status = d.get("status", ConnectorHealth.HEALTHY)
        if not isinstance(status, str):
            raise ValueError("invalid field 'health.status': expected string")
        if status not in (ConnectorHealth.HEALTHY, ConnectorHealth.UNHEALTHY):
            raise ValueError(
                "invalid field 'health.status': expected 'Healthy' or 'Unhealthy'"
            )
        health.status = status
        description = d.get("description")
        if description is not None and not isinstance(description, str):
            raise ValueError(
                "invalid optional field 'health.description': expected string or null"
            )
        health.description = description
        return health

class CompletedWatermark:
    """Latest completed watermark reported by input connector status."""

    def __init__(self):
        self.metadata: Any = None
        self.ingested_at: datetime = datetime.fromtimestamp(0)
        self.processed_at: datetime = datetime.fromtimestamp(0)
        self.completed_at: datetime = datetime.fromtimestamp(0)

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        watermark = cls()
        if "metadata" not in d:
            raise ValueError("missing required field 'metadata'")
        watermark.metadata = d.get("metadata")
        watermark.ingested_at = parse_datetime(
            expect_str(d, "ingested_at"), "ingested_at"
        )
        watermark.processed_at = parse_datetime(
            expect_str(d, "processed_at"), "processed_at"
        )
        watermark.completed_at = parse_datetime(
            expect_str(d, "completed_at"), "completed_at"
        )
        return watermark


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
        self.parse_errors: Optional[list[ConnectorError]] = None
        self.transport_errors: Optional[list[ConnectorError]] = None
        self.health: Optional[ConnectorHealth] = None
        self.paused: Optional[bool] = None
        self.barrier: Optional[bool] = None
        self.completed_frontier: Optional[CompletedWatermark] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.endpoint_name = expect_str(d, "endpoint_name")
        status.config = expect_mapping(d, "config")
        status.metrics = InputEndpointMetrics.from_dict(expect_mapping(d, "metrics"))
        fatal_error = d.get("fatal_error")
        if fatal_error is not None and not isinstance(fatal_error, str):
            raise ValueError(
                "invalid optional field 'fatal_error': expected string or null"
            )
        status.fatal_error = fatal_error
        status.paused = expect_bool(d, "paused")
        status.barrier = expect_bool(d, "barrier")
        parse_errors = d.get("parse_errors")
        status.parse_errors = (
            [ConnectorError.from_dict(error) for error in parse_errors]
            if isinstance(parse_errors, list)
            else None
        )
        transport_errors = d.get("transport_errors")
        status.transport_errors = (
            [ConnectorError.from_dict(error) for error in transport_errors]
            if isinstance(transport_errors, list)
            else None
        )
        health = d.get("health")
        status.health = (
            ConnectorHealth.from_dict(health) if isinstance(health, Mapping) else None
        )
        completed_frontier = d.get("completed_frontier")
        status.completed_frontier = (
            CompletedWatermark.from_dict(completed_frontier)
            if isinstance(completed_frontier, dict)
            else None
        )
        return status


class InputEndpointMetrics:
    """Represents the "metrics" member within an input endpoint status
    in the pipeline's "/stats" endpoint reply.
    """

    def __init__(self):
        self.total_bytes: Optional[int] = None
        self.total_records: Optional[int] = None
        self.buffered_records: Optional[int] = None
        self.buffered_bytes: Optional[int] = None
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
        self.encode_errors: Optional[list[ConnectorError]] = None
        self.transport_errors: Optional[list[ConnectorError]] = None
        self.health: Optional[ConnectorHealth] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.endpoint_name = expect_str(d, "endpoint_name")
        status.config = expect_mapping(d, "config")
        status.metrics = OutputEndpointMetrics.from_dict(expect_mapping(d, "metrics"))
        fatal_error = d.get("fatal_error")
        if fatal_error is not None and not isinstance(fatal_error, str):
            raise ValueError(
                "invalid optional field 'fatal_error': expected string or null"
            )
        status.fatal_error = fatal_error
        encode_errors = d.get("encode_errors")
        status.encode_errors = (
            [ConnectorError.from_dict(error) for error in encode_errors]
            if isinstance(encode_errors, list)
            else None
        )
        transport_errors = d.get("transport_errors")
        status.transport_errors = (
            [ConnectorError.from_dict(error) for error in transport_errors]
            if isinstance(transport_errors, list)
            else None
        )
        health = d.get("health")
        status.health = (
            ConnectorHealth.from_dict(health) if isinstance(health, Mapping) else None
        )
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
        self.buffered_records: Optional[int] = None
        self.buffered_batches: Optional[int] = None
        self.num_encode_errors: Optional[int] = None
        self.num_transport_errors: Optional[int] = None
        self.total_processed_input_records: Optional[int] = None
        self.total_processed_steps: Optional[int] = None
        self.memory: Optional[int] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        metrics = cls()
        metrics.__dict__.update(d)
        return metrics


class CommitProgressSummary:
    """Progress of a transaction commit."""

    def __init__(self):
        """Initializes an empty status."""
        self.completed: Optional[int] = None
        self.in_progress: Optional[int] = None
        self.remaining: Optional[int] = None
        self.in_progress_processed_records: Optional[int] = None
        self.in_progress_total_records: Optional[int] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.__dict__.update(d)
        return status


class TransactionInitiators:
    """Initiators for an ongoing transaction."""

    def __init__(self):
        """Initializes an empty status."""
        self.transaction_id: Optional[int] = None
        self.initiated_by_api: Optional[str] = None
        self.initiated_by_connectors: Optional[
            Mapping[str, ConnectorTransactionPhase]
        ] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.__dict__.update(d)
        status.initiated_by_connectors = ConnectorTransactionPhase.from_dict(
            d["initiated_by_connectors"]
        )
        return status


class ConnectorTransactionPhase:
    """Connector transaction phase with optional label"""

    def __init__(self):
        """Initializes an empty status."""
        self.phase: Optional[str] = None
        self.label: Optional[str] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.__dict__.update(d)
        return status
