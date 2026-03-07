from datetime import datetime, timezone
from typing import Any, Mapping, Optional

from feldera._helpers import (
    expect_bool,
    expect_int,
    expect_mapping,
    expect_str,
    parse_datetime,
)


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
        error.timestamp = parse_datetime(
            expect_str(d, "timestamp"), "timestamp"
        )
        error.index = expect_int(d, "index")
        tag = d.get("tag")
        if tag is not None and not isinstance(tag, str):
            raise ValueError("invalid optional field 'tag': expected string or null")
        error.tag = tag
        error.message = expect_str(d, "message")
        return error


class ShortEndpointConfig:
    """Endpoint configuration subset returned by connector status endpoints."""

    def __init__(self):
        self.stream: str = ""

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        config = cls()
        config.stream = expect_str(d, "stream")
        return config


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


class InputConnectorMetrics:
    """Performance metrics returned by an input connector status endpoint."""

    def __init__(self):
        self.total_bytes: int = 0
        self.total_records: int = 0
        self.buffered_records: int = 0
        self.buffered_bytes: int = 0
        self.num_transport_errors: int = 0
        self.num_parse_errors: int = 0
        self.end_of_input: bool = False

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        metrics = cls()
        metrics.total_bytes = expect_int(d, "total_bytes")
        metrics.total_records = expect_int(d, "total_records")
        metrics.buffered_records = expect_int(d, "buffered_records")
        metrics.buffered_bytes = expect_int(d, "buffered_bytes")
        metrics.num_transport_errors = expect_int(d, "num_transport_errors")
        metrics.num_parse_errors = expect_int(d, "num_parse_errors")
        metrics.end_of_input = expect_bool(d, "end_of_input")
        return metrics


class OutputConnectorMetrics:
    """Performance metrics returned by an output connector status endpoint."""

    def __init__(self):
        self.transmitted_records: int = 0
        self.transmitted_bytes: int = 0
        self.queued_records: int = 0
        self.queued_batches: int = 0
        self.buffered_records: int = 0
        self.buffered_batches: int = 0
        self.num_encode_errors: int = 0
        self.num_transport_errors: int = 0
        self.total_processed_input_records: int = 0
        self.total_processed_steps: int = 0
        self.memory: int = 0

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        metrics = cls()
        metrics.transmitted_records = expect_int(d, "transmitted_records")
        metrics.transmitted_bytes = expect_int(d, "transmitted_bytes")
        metrics.queued_records = expect_int(d, "queued_records")
        metrics.queued_batches = expect_int(d, "queued_batches")
        metrics.buffered_records = expect_int(d, "buffered_records")
        metrics.buffered_batches = expect_int(d, "buffered_batches")
        metrics.num_encode_errors = expect_int(d, "num_encode_errors")
        metrics.num_transport_errors = expect_int(d, "num_transport_errors")
        metrics.total_processed_input_records = expect_int(
            d, "total_processed_input_records"
        )
        metrics.total_processed_steps = expect_int(d, "total_processed_steps")
        metrics.memory = expect_int(d, "memory")
        return metrics


class InputConnectorStatus:
    """Mirrors Rust `ExternalInputEndpointStatus`."""

    def __init__(self):
        self.endpoint_name: str = ""
        self.config: ShortEndpointConfig = ShortEndpointConfig()
        self.metrics: InputConnectorMetrics = InputConnectorMetrics()
        self.fatal_error: Optional[str] = None
        self.parse_errors: Optional[list[ConnectorError]] = None
        self.transport_errors: Optional[list[ConnectorError]] = None
        self.paused: bool = False
        self.barrier: bool = False
        self.completed_frontier: Optional[CompletedWatermark] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.endpoint_name = expect_str(d, "endpoint_name")
        status.config = ShortEndpointConfig.from_dict(expect_mapping(d, "config"))
        status.metrics = InputConnectorMetrics.from_dict(expect_mapping(d, "metrics"))
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
        completed_frontier = d.get("completed_frontier")
        status.completed_frontier = (
            CompletedWatermark.from_dict(completed_frontier)
            if isinstance(completed_frontier, dict)
            else None
        )
        return status


class OutputConnectorStatus:
    """Mirrors Rust `ExternalOutputEndpointStatus`."""

    def __init__(self):
        self.endpoint_name: str = ""
        self.config: ShortEndpointConfig = ShortEndpointConfig()
        self.metrics: OutputConnectorMetrics = OutputConnectorMetrics()
        self.fatal_error: Optional[str] = None
        self.encode_errors: Optional[list[ConnectorError]] = None
        self.transport_errors: Optional[list[ConnectorError]] = None

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        status = cls()
        status.endpoint_name = expect_str(d, "endpoint_name")
        status.config = ShortEndpointConfig.from_dict(expect_mapping(d, "config"))
        status.metrics = OutputConnectorMetrics.from_dict(expect_mapping(d, "metrics"))
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
        return status


# Backward-compatible alias for a common typo.
OutputConnectrorStatus = OutputConnectorStatus
