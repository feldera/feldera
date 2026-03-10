"""
Benchmarking utilities for Feldera pipelines.

Provides functionality to collect pipeline performance metrics, format them as
Bencher Metric Format (BMF), and upload results to a Bencher-compatible server.

This mirrors the `fda bench --upload` CLI functionality so Python-based benchmark
workloads can collect and upload results programmatically.

**Differences from** ``fda bench``: throughput, uptime, and state-amplification
are computed as deltas between the first and last collected sample (i.e. over the
observation window), whereas ``fda bench`` uses absolute values since pipeline
start.  This means the two tools will report different numbers for the same run,
but the Python SDK approach is more appropriate when benchmarking a pipeline that
may have been running before collection started.
"""

import json
import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

import requests

if TYPE_CHECKING:
    from feldera.pipeline import Pipeline
    from feldera.rest.feldera_client import FelderaClient

logger = logging.getLogger(__name__)

POLL_INTERVAL_S = 0.25


class CompletionCondition(Enum):
    """Strategy for determining when benchmark collection should stop.

    :cvar PIPELINE_COMPLETE: Stop when the pipeline's ``pipeline_complete``
        flag becomes ``True``.  This is the default and works for finite input
        connectors (e.g. files, HTTP) that signal end-of-input to the pipeline.
    :cvar IDLE: Stop when ``total_processed_records`` has been stable (unchanged)
        for a configurable idle interval **and** at least one record has been
        processed.  Use this for streaming connectors like Kafka that never
        signal end-of-input even after all available data has been consumed.
    """

    PIPELINE_COMPLETE = "pipeline_complete"
    IDLE = "idle"


def _human_readable_bytes(n: int) -> str:
    """Format a byte count as a human-readable string."""
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(n)
    exp = 0
    while value >= 1024 and exp < len(units) - 1:
        value /= 1024
        exp += 1
    return f"{value:.2f} {units[exp]}"


def _stddev(values: list[float]) -> float:
    """Population standard deviation."""
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    return math.sqrt(sum((x - mean) ** 2 for x in values) / n)


def _averaged_metrics(runs: list["BenchmarkResult"]) -> "BenchmarkMetrics":
    """Compute averaged BenchmarkMetrics from a list of runs.

    Averages: throughput, uptime_ms, buffered_input_records_avg, state_amplification
    Min-of-mins: memory_bytes_min, storage_bytes_min, buffered_input_records_min
    Max-of-maxes: memory_bytes_max, storage_bytes_max, buffered_input_records_max
    """
    n = len(runs)
    metrics_list = [r.metrics for r in runs]

    sa_values = [
        m.state_amplification for m in metrics_list if m.state_amplification is not None
    ]

    return BenchmarkMetrics(
        throughput=int(sum(m.throughput for m in metrics_list) / n),
        memory_bytes_max=max(m.memory_bytes_max for m in metrics_list),
        memory_bytes_min=min(m.memory_bytes_min for m in metrics_list),
        storage_bytes_max=max(m.storage_bytes_max for m in metrics_list),
        storage_bytes_min=min(m.storage_bytes_min for m in metrics_list),
        uptime_ms=int(sum(m.uptime_ms for m in metrics_list) / n),
        buffered_input_records_avg=int(
            sum(m.buffered_input_records_avg for m in metrics_list) / n
        ),
        buffered_input_records_min=min(
            m.buffered_input_records_min for m in metrics_list
        ),
        buffered_input_records_max=max(
            m.buffered_input_records_max for m in metrics_list
        ),
        state_amplification=sum(sa_values) / len(sa_values) if sa_values else None,
    )


@dataclass
class RawSample:
    """One stats snapshot from ``pipeline.stats()``.

    :param rss_bytes: Resident set size of the pipeline process in bytes.
    :param uptime_msecs: Real-world elapsed milliseconds since the pipeline
        process started (wall-clock time, not aggregated CPU time).
    :param incarnation_uuid: UUID identifying the current pipeline incarnation.
        Changes if the pipeline restarts.
    :param storage_bytes: Bytes currently stored by the pipeline.
    :param buffered_input_records: Number of records buffered in input connectors.
    :param total_processed_records: Cumulative number of records processed since
        pipeline start.
    :param input_bytes: Total bytes received across all input connectors.
    :param input_errors: ``True`` if any input connector reported a fatal error,
        parse error, or transport error.
    """

    rss_bytes: int
    uptime_msecs: int
    incarnation_uuid: str
    storage_bytes: int
    buffered_input_records: int
    total_processed_records: int
    input_bytes: int
    input_errors: bool

    @classmethod
    def from_pipeline_statistics(cls, stats) -> "RawSample":
        """Construct a :class:`RawSample` from a
        :class:`~feldera.stats.PipelineStatistics` object.

        :param stats: A :class:`~feldera.stats.PipelineStatistics` instance
            returned by :meth:`~feldera.pipeline.Pipeline.stats`.
        :returns: A new :class:`RawSample` populated from *stats*.
        """
        gm = stats.global_metrics

        input_bytes = sum(
            ep.metrics.total_bytes
            for ep in stats.inputs
            if ep.metrics is not None and ep.metrics.total_bytes is not None
        )

        input_errors = any(
            (ep.fatal_error is not None)
            or (ep.metrics is not None and (ep.metrics.num_transport_errors or 0) > 0)
            or (ep.metrics is not None and (ep.metrics.num_parse_errors or 0) > 0)
            for ep in stats.inputs
        )

        return cls(
            rss_bytes=gm.rss_bytes or 0,
            uptime_msecs=int(gm.uptime_msecs or 0),
            incarnation_uuid=str(gm.incarnation_uuid),
            storage_bytes=gm.storage_bytes or 0,
            buffered_input_records=gm.buffered_input_records or 0,
            total_processed_records=gm.total_processed_records or 0,
            input_bytes=input_bytes,
            input_errors=input_errors,
        )


@dataclass
class BenchmarkMetrics:
    """Aggregated benchmark metrics derived from a list of :class:`RawSample`.

    :param throughput: Records processed per second during the measurement
        window, computed as a delta between the first and last samples.
    :param memory_bytes_max: Peak RSS memory usage in bytes across all samples.
    :param memory_bytes_min: Minimum RSS memory usage in bytes across all samples.
    :param storage_bytes_max: Peak storage usage in bytes across all samples.
    :param storage_bytes_min: Minimum storage usage in bytes across all samples.
    :param uptime_ms: Real-world elapsed milliseconds of the measurement window
        (delta of ``uptime_msecs`` between first and last sample).
    :param buffered_input_records_avg: Average buffered input record count across
        all samples (integer division).
    :param buffered_input_records_min: Minimum buffered input record count across
        all samples.
    :param buffered_input_records_max: Maximum buffered input record count across
        all samples.
    :param state_amplification: Ratio of peak storage bytes to input bytes
        received during the measurement window. ``None`` when delta input
        bytes is zero.
    """

    throughput: int
    memory_bytes_max: int
    memory_bytes_min: int
    storage_bytes_max: int
    storage_bytes_min: int
    uptime_ms: int
    buffered_input_records_avg: int
    buffered_input_records_min: int
    buffered_input_records_max: int
    state_amplification: Optional[float]

    @classmethod
    def from_samples(cls, samples: list) -> "BenchmarkMetrics":
        """Aggregate a list of :class:`RawSample` objects into benchmark metrics.

        :param samples: Non-empty list of :class:`RawSample` instances collected
            during a benchmark run.
        :returns: A new :class:`BenchmarkMetrics` instance.
        :raises ValueError: If *samples* is empty.
        """
        if not samples:
            raise ValueError(
                "No measurements were recorded. Maybe try to increase `duration`."
            )

        first = samples[0]
        last = samples[-1]

        # Delta-based: measure only the records and time within the window
        delta_records = last.total_processed_records - first.total_processed_records
        delta_ms = last.uptime_msecs - first.uptime_msecs
        delta_s = delta_ms / 1000.0
        throughput = int(delta_records / delta_s) if delta_s > 0 else 0

        memory_bytes_max = max(s.rss_bytes for s in samples)
        memory_bytes_min = min(s.rss_bytes for s in samples)

        storage_bytes_max = max(s.storage_bytes for s in samples)
        storage_bytes_min = min(s.storage_bytes for s in samples)

        buf_values = [s.buffered_input_records for s in samples]
        buffered_input_records_avg = sum(buf_values) // len(buf_values)
        buffered_input_records_min = min(buf_values)
        buffered_input_records_max = max(buf_values)

        if buffered_input_records_min == 0:
            zero_count = sum(1 for b in buf_values if b == 0)
            logger.warning(
                "Input buffering was 0 for %d samples; the pipeline may not be "
                "receiving enough data to evaluate its true performance.",
                zero_count,
            )

        delta_input_bytes = last.input_bytes - first.input_bytes
        state_amplification = (
            storage_bytes_max / delta_input_bytes if delta_input_bytes > 0 else None
        )

        return cls(
            throughput=throughput,
            memory_bytes_max=memory_bytes_max,
            memory_bytes_min=memory_bytes_min,
            storage_bytes_max=storage_bytes_max,
            storage_bytes_min=storage_bytes_min,
            uptime_ms=delta_ms,
            buffered_input_records_avg=buffered_input_records_avg,
            buffered_input_records_min=buffered_input_records_min,
            buffered_input_records_max=buffered_input_records_max,
            state_amplification=state_amplification,
        )


@dataclass
class BenchmarkResult:
    """A named benchmark result with timing information.

    For a single run, ``_metrics`` is set directly. For multi-run aggregation,
    ``runs`` holds the individual results and ``metrics`` computes averages
    on the fly.

    :param name: Benchmark name, used as the top-level key in BMF output.
    :param start_time: UTC timestamp when metric collection began.
    :param end_time: UTC timestamp when metric collection ended.
    :param runs: List of individual run results (multi-run aggregation).
    :param _metrics: Aggregated performance metrics (single run).
    """

    name: str
    start_time: datetime
    end_time: datetime
    runs: list["BenchmarkResult"] | None = None
    _metrics: BenchmarkMetrics | None = field(default=None, repr=False)

    @property
    def metrics(self) -> BenchmarkMetrics:
        """Return metrics — stored for single run, computed for multi-run."""
        if self._metrics is not None:
            return self._metrics
        if self.runs:
            return _averaged_metrics(self.runs)
        raise ValueError("BenchmarkResult has no metrics and no runs")

    @classmethod
    def aggregate(
        cls, results: list["BenchmarkResult"], name: str | None = None
    ) -> "BenchmarkResult":
        """Create an aggregated result from multiple runs.

        :param results: Non-empty list of individual run results.
        :param name: Optional name override; defaults to the first result's name.
        :returns: A new :class:`BenchmarkResult` with ``runs`` set.
        """
        return cls(
            name=name or results[0].name,
            start_time=min(r.start_time for r in results),
            end_time=max(r.end_time for r in results),
            runs=results,
        )

    def to_bmf(self) -> dict:
        """Return the result as a Bencher Metric Format (BMF) dict.

        :returns: A dict ``{name: {metric_name: {value, ...}, ...}}`` suitable
            for serialising to JSON and submitting to a Bencher-compatible server.
        """
        m = self.metrics
        entry: dict = {
            "throughput": {"value": m.throughput},
            "memory": {
                "value": m.memory_bytes_max,
                "lower_value": m.memory_bytes_min,
            },
            "storage": {
                "value": m.storage_bytes_max,
                "lower_value": m.storage_bytes_min,
            },
            "uptime": {"value": m.uptime_ms},
            "buffered-input-records": {
                "value": m.buffered_input_records_avg,
                "lower_value": m.buffered_input_records_min,
                "upper_value": m.buffered_input_records_max,
            },
        }
        if m.state_amplification is not None:
            entry["state-amplification"] = {"value": m.state_amplification}
        return {self.name: entry}

    def to_json(self) -> str:
        """Return the BMF dict serialised as a pretty-printed JSON string.

        :returns: A JSON string representation of :meth:`to_bmf`.
        """
        return json.dumps(self.to_bmf(), indent=2)

    def format_table(self) -> str:
        """Return a human-readable tabular display of the benchmark results.

        For multi-run results (``self.runs is not None``), value cells show
        ``avg (stddev X.Y%)`` computed from per-run metrics.

        :returns: A multi-line string containing an ASCII table with one row
            per metric.
        """
        m = self.metrics
        is_multi = self.runs is not None and len(self.runs) > 1

        def _val_with_stddev(avg: float, values: list[float], fmt: str = ".0f") -> str:
            if not is_multi:
                return f"{avg:{fmt}}"
            sd = _stddev(values)
            pct = (sd / avg * 100) if avg != 0 else 0.0
            return f"{avg:{fmt}} (stddev {pct:.1f}%)"

        def _bytes_with_stddev(avg: int, values: list[int]) -> str:
            if not is_multi:
                return _human_readable_bytes(avg)
            sd = _stddev([float(v) for v in values])
            pct = (sd / avg * 100) if avg != 0 else 0.0
            return f"{_human_readable_bytes(avg)} (stddev {pct:.1f}%)"

        rows: list[tuple[str, str]] = [
            ("Metric", "Value"),
            (
                "Throughput (records/s)",
                _val_with_stddev(
                    m.throughput,
                    [r.metrics.throughput for r in self.runs] if is_multi else [],
                ),
            ),
            (
                "Memory",
                _bytes_with_stddev(
                    m.memory_bytes_max,
                    [r.metrics.memory_bytes_max for r in self.runs] if is_multi else [],
                ),
            ),
            (
                "Storage",
                _bytes_with_stddev(
                    m.storage_bytes_max,
                    [r.metrics.storage_bytes_max for r in self.runs]
                    if is_multi
                    else [],
                ),
            ),
            (
                "Uptime [ms]",
                _val_with_stddev(
                    m.uptime_ms,
                    [r.metrics.uptime_ms for r in self.runs] if is_multi else [],
                ),
            ),
        ]
        if m.state_amplification is not None:
            rows.append(
                (
                    "State Amplification",
                    _val_with_stddev(
                        m.state_amplification,
                        [
                            r.metrics.state_amplification
                            for r in self.runs
                            if r.metrics.state_amplification is not None
                        ]
                        if is_multi
                        else [],
                        fmt=".2f",
                    ),
                )
            )

        col_widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
        sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"

        n_runs = len(self.runs) if self.runs else 1
        header = f"Benchmark Results ({n_runs} run{'s' if n_runs != 1 else ''}):"
        lines = [header, sep]
        for i, row in enumerate(rows):
            line = (
                "| "
                + " | ".join(cell.ljust(col_widths[j]) for j, cell in enumerate(row))
                + " |"
            )
            lines.append(line)
            if i == 0:
                lines.append(sep)
        lines.append(sep)
        return "\n".join(lines)


def collect_metrics(
    pipeline: "Pipeline",
    duration_secs: Optional[float] = None,
    completion_condition: CompletionCondition = CompletionCondition.PIPELINE_COMPLETE,
    idle_interval_s: float = 1.0,
) -> tuple:
    """
    Poll pipeline stats until completion or ``duration_secs`` elapses.

    :param pipeline: A running :class:`~feldera.pipeline.Pipeline`.
    :param duration_secs: Optional maximum collection duration in seconds.
        If ``None``, polling continues until the completion condition is met.
    :param completion_condition: Strategy for detecting completion.
        See :class:`CompletionCondition`.
    :param idle_interval_s: When using :attr:`CompletionCondition.IDLE`,
        the number of seconds ``total_processed_records`` must remain unchanged
        before collection stops.  Ignored for other conditions.
    :returns: ``(samples, start_time, end_time)`` where *samples* is a list
        of :class:`RawSample` objects.
    :raises RuntimeError: If any input connector reported errors during collection.
    """
    samples: list[RawSample] = []
    start_time = datetime.now(timezone.utc).replace(tzinfo=None)
    loop_start = time.monotonic()
    first_uuid: Optional[str] = None

    # State for IDLE completion tracking
    idle_started_at: Optional[float] = None
    prev_processed: Optional[int] = None

    while True:
        stats = pipeline.stats()
        sample = RawSample.from_pipeline_statistics(stats)
        samples.append(sample)

        logger.info("Collected metrics at %.1fs", time.monotonic() - loop_start)

        # Validate incarnation UUID consistency
        if first_uuid is None:
            first_uuid = sample.incarnation_uuid
        elif sample.incarnation_uuid != first_uuid:
            logger.warning(
                "Inconsistent incarnation_uuid detected during benchmark "
                "(was %s, now %s). Did the pipeline restart while measuring?",
                first_uuid,
                sample.incarnation_uuid,
            )

        # Check completion based on the chosen strategy
        if completion_condition == CompletionCondition.PIPELINE_COMPLETE:
            if stats.global_metrics.pipeline_complete:
                logger.info("Pipeline completed, stopping benchmark collection.")
                break
        elif completion_condition == CompletionCondition.IDLE:
            now = time.monotonic()
            cur_processed = sample.total_processed_records
            if (
                prev_processed is not None
                and cur_processed == prev_processed
                and cur_processed > 0
            ):
                if idle_started_at is None:
                    idle_started_at = now
                elif now - idle_started_at >= idle_interval_s:
                    logger.info(
                        "Pipeline idle for %.1fs at %d records processed, "
                        "stopping benchmark collection.",
                        idle_interval_s,
                        cur_processed,
                    )
                    break
            else:
                idle_started_at = None
            prev_processed = cur_processed

        # Stop when duration limit reached
        if duration_secs is not None and time.monotonic() - loop_start >= duration_secs:
            logger.info("Reached duration limit of %.1fs.", duration_secs)
            break

        time.sleep(POLL_INTERVAL_S)

    end_time = datetime.now(timezone.utc).replace(tzinfo=None)

    if any(s.input_errors for s in samples):
        raise RuntimeError(
            "Detected errors in input connectors during benchmark collection. "
            "Check pipeline logs for details."
        )

    return samples, start_time, end_time


def bench(
    pipeline: "Pipeline",
    name: Optional[str] = None,
    duration_secs: Optional[float] = None,
    completion_condition: CompletionCondition = CompletionCondition.PIPELINE_COMPLETE,
    idle_interval_s: float = 5.0,
) -> BenchmarkResult:
    """
    Collect benchmark metrics from a running pipeline and return a result.

    :param pipeline: A running :class:`~feldera.pipeline.Pipeline`.
    :param name: Benchmark name. Defaults to the pipeline name.
    :param duration_secs: Optional maximum collection duration in seconds.
        Acts as an upper bound regardless of *completion_condition*.
    :param completion_condition: Strategy for detecting completion.
        See :class:`CompletionCondition`.
    :param idle_interval_s: Seconds of stable ``total_processed_records``
        before :attr:`CompletionCondition.IDLE` triggers.  Ignored otherwise.
    :returns: A :class:`BenchmarkResult` with collected metrics.
    """
    benchmark_name = name if name is not None else pipeline.name
    samples, start_time, end_time = collect_metrics(
        pipeline, duration_secs, completion_condition, idle_interval_s
    )
    metrics = BenchmarkMetrics.from_samples(samples)
    return BenchmarkResult(
        name=benchmark_name,
        start_time=start_time,
        end_time=end_time,
        _metrics=metrics,
    )


def upload_to_bencher(
    result: BenchmarkResult,
    project: Optional[str] = None,
    *,
    host: Optional[str] = None,
    token: Optional[str] = None,
    branch: str = "main",
    testbed: Optional[str] = None,
    git_hash: Optional[str] = None,
    start_point: Optional[str] = None,
    start_point_hash: Optional[str] = None,
    start_point_max_versions: int = 255,
    start_point_clone_thresholds: bool = False,
    start_point_reset: bool = False,
    feldera_client: Optional["FelderaClient"] = None,
) -> requests.Response:
    """
    Upload a :class:`BenchmarkResult` to a Bencher-compatible server.

    Environment variables (used as defaults when the corresponding parameter is
    ``None``):

    - ``BENCHER_API_TOKEN`` — API token for authentication.
    - ``BENCHER_PROJECT``   — Project slug on the Bencher server.
    - ``BENCHER_HOST``      — Base URL of the Bencher server.

    :param result: The benchmark result to upload.
    :param project: Bencher project slug. Defaults to the ``BENCHER_PROJECT``
        environment variable.
    :param host: Bencher server base URL. Defaults to the ``BENCHER_HOST``
        environment variable, or ``"https://benchmarks.feldera.io"``.
    :param token: Bencher API token. Defaults to the ``BENCHER_API_TOKEN``
        environment variable.
    :param branch: Branch name to report the run under. Defaults to ``"main"``.
    :param testbed: Testbed name. When ``None`` and *feldera_client* is provided,
        the hostname of the Feldera instance is used.
    :param git_hash: Optional git commit hash associated with this run.
    :param start_point: Optional branch name to use as the start point.
    :param start_point_hash: Optional git hash for the start point.
    :param start_point_max_versions: Maximum number of start point versions to
        consider. Defaults to 255.
    :param start_point_clone_thresholds: Whether to clone thresholds from the
        start point branch.
    :param start_point_reset: Whether to reset the start point.
    :param feldera_client: Optional :class:`~feldera.rest.feldera_client.FelderaClient`
        used to enrich the run context with edition and revision information.
    :returns: The :class:`requests.Response` from the Bencher server.
    :raises ValueError: If *project* is ``None`` and ``BENCHER_PROJECT`` is not set.
    :raises RuntimeError: If the server returns a non-2xx status code.
    """
    resolved_host = (
        host or os.environ.get("BENCHER_HOST") or "https://benchmarks.feldera.io"
    )
    resolved_token = token or os.environ.get("BENCHER_API_TOKEN")
    resolved_project = project or os.environ.get("BENCHER_PROJECT")

    if resolved_project is None:
        raise ValueError(
            "project must be provided either as a parameter or via the "
            "BENCHER_PROJECT environment variable."
        )

    # Build run context
    context: dict = {
        "bencher.dev/v0/branch/ref/name": branch,
    }

    # Resolve testbed and enrich context from Feldera instance config
    resolved_testbed = testbed
    if feldera_client is not None:
        try:
            config = feldera_client.get_config()
            context["bencher.dev/v0/repo/name"] = f"feldera {config.edition.value}"
            context["bencher.dev/v0/branch/hash"] = config.revision

            edition = config.edition.value
            if edition == "Open source":
                context["bencher.dev/v0/repo/hash"] = (
                    "de8879fbda0c9e9392e3b94064c683a1b4bae216"
                )
            elif edition == "Enterprise":
                context["bencher.dev/v0/repo/hash"] = (
                    "751db38ff821d73bcc67c836af421d76d4d42bdd"
                )
            else:
                logger.warning(
                    "Unknown Feldera edition '%s'; not setting repo hash.", edition
                )

            # Auto-resolve git_hash from runtime_revision if not provided by caller,
            # mirroring the fda bench logic (runtime_version takes precedence, then
            # runtime_revision).
            if git_hash is None and config.runtime_revision:
                git_hash = config.runtime_revision
        except Exception as exc:
            logger.warning("Failed to fetch Feldera instance config: %s", exc)

        if resolved_testbed is None:
            instance_url = feldera_client.config.url
            parsed = urlparse(instance_url)
            resolved_testbed = parsed.hostname or instance_url

    # Build payload
    payload: dict = {
        "branch": branch,
        "project": resolved_project,
        "context": context,
        "start_time": result.start_time.isoformat() + "Z",
        "end_time": result.end_time.isoformat() + "Z",
        "results": [result.to_json()],
        "settings": {"adapter": "json"},
        "thresholds": None,
    }

    if git_hash is not None:
        payload["hash"] = git_hash

    if resolved_testbed is not None:
        payload["testbed"] = resolved_testbed

    if start_point is not None:
        sp: dict = {
            "branch": start_point,
            "clone_thresholds": start_point_clone_thresholds,
            "max_versions": start_point_max_versions,
            "reset": start_point_reset,
        }
        if start_point_hash is not None:
            sp["hash"] = start_point_hash
        payload["start_point"] = sp
    else:
        payload["start_point"] = None

    # Build headers
    headers: dict = {}
    if resolved_token:
        headers["Authorization"] = f"Bearer {resolved_token}"
    else:
        logger.warning(
            "No Bencher API token provided; attempting upload without authentication."
        )

    url = f"{resolved_host.rstrip('/')}/v0/run"
    response = requests.post(url, json=payload, headers=headers, timeout=15)

    if not response.ok:
        raise RuntimeError(
            f"Failed to upload benchmark result: HTTP {response.status_code} — {response.text}"
        )

    logger.info("Benchmark result uploaded successfully.")
    return response
