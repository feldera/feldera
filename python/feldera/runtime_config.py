import warnings
from typing import Any, Mapping, Optional

from feldera.enums import FaultToleranceModel


class Resources:
    """
    Class used to specify the resource configuration for a pipeline.

    :param config: A dictionary containing all the configuration values.
    :param cpu_cores_max: The maximum number of CPU cores to reserve for an instance of the pipeline.
    :param cpu_cores_min: The minimum number of CPU cores to reserve for an instance of the pipeline.
    :param memory_mb_max: The maximum memory in Megabytes to reserve for an instance of the pipeline.
    :param memory_mb_min: The minimum memory in Megabytes to reserve for an instance of the pipeline.
    :param storage_class: The storage class to use for the pipeline. The class determines storage performance such
        as IOPS and throughput.
    :param storage_mb_max: The  storage in Megabytes to reserve for an instance of the pipeline.
    """

    def __init__(
        self,
        config: Optional[Mapping[str, Any]] = None,
        cpu_cores_max: Optional[int] = None,
        cpu_cores_min: Optional[int] = None,
        memory_mb_max: Optional[int] = None,
        memory_mb_min: Optional[int] = None,
        storage_class: Optional[str] = None,
        storage_mb_max: Optional[int] = None,
    ):
        config = config or {}

        self.cpu_cores_max = cpu_cores_max
        self.cpu_cores_min = cpu_cores_min
        self.memory_mb_max = memory_mb_max
        self.memory_mb_min = memory_mb_min
        self.storage_class = storage_class
        self.storage_mb_max = storage_mb_max

        self.__dict__.update(config)


class Storage:
    """Storage configuration for a pipeline.

    :param min_storage_bytes: The minimum estimated number of bytes in a batch of data to write it to storage.
    """

    def __init__(
        self,
        config: Optional[Mapping[str, Any]] = None,
        min_storage_bytes: Optional[int] = None,
    ):
        config = config or {}

        self.min_storage_bytes = min_storage_bytes

        self.__dict__.update(config)


def _duration_setting(
    new_name: str,
    new_value: Optional[str],
    old_name: str,
    old_value: Optional[int],
    unit: str,
) -> Optional[str]:
    """Resolves a duration setting given through either its current name or the
    deprecated integer field it replaces.

    The current name wins. Either use of the deprecated field warns, so that a
    caller who passes both learns that only one of the two took effect.
    """
    if old_value is None:
        return new_value
    equivalent = f"{old_value}{unit}"
    if new_value is None:
        warnings.warn(
            f"'{old_name}' is deprecated; use {new_name}='{equivalent}' instead",
            DeprecationWarning,
            stacklevel=3,
        )
        return equivalent
    warnings.warn(
        f"'{old_name}' is deprecated and was ignored because "
        f"{new_name}='{new_value}' is also set; drop "
        f"{old_name}={old_value} (it would mean '{equivalent}')",
        DeprecationWarning,
        stacklevel=3,
    )
    return new_value


class RuntimeConfig:
    """
    Runtime configuration class to define the configuration for a pipeline.
    To create runtime config from a dictionary, use
    :meth:`.RuntimeConfig.from_dict`.

    Duration settings take a string made of a number and a unit, such as
    ``"500ms"``, ``"30s"``, ``"1h30m"`` or ``"30d"``. The integer fields they
    replace (``max_buffering_delay_usecs``, ``clock_resolution_usecs``,
    ``provisioning_timeout_secs`` and ``checkpoint_interval_secs``) still work
    but emit a :class:`DeprecationWarning`.

    Documentation:
        https://docs.feldera.com/pipelines/configuration/#runtime-configuration
    """

    def __init__(
        self,
        workers: Optional[int] = None,
        hosts: Optional[int] = None,
        storage: Optional[Storage | bool] = None,
        tracing: Optional[bool] = False,
        tracing_endpoint_jaeger: Optional[str] = "",
        cpu_profiler: bool = True,
        max_buffering_delay_usecs: Optional[int] = None,
        min_batch_size_records: int = 0,
        clock_resolution_usecs: Optional[int] = None,
        clock_timezone_offset: Optional[str] = None,
        provisioning_timeout_secs: Optional[int] = None,
        resources: Optional[Resources] = None,
        fault_tolerance_model: Optional[FaultToleranceModel] = None,
        checkpoint_interval_secs: Optional[int] = None,
        dev_tweaks: Optional[dict] = None,
        env: Optional[dict[str, str]] = None,
        logging: Optional[str] = None,
        datafusion_memory_mb: Optional[int] = None,
        max_rss_mb: Optional[int] = None,
        max_buffering_delay: Optional[str] = None,
        clock_resolution: Optional[str] = None,
        provisioning_timeout: Optional[str] = None,
        checkpoint_interval: Optional[str] = None,
    ):
        self.workers = workers
        self.hosts = hosts
        self.datafusion_memory_mb = datafusion_memory_mb
        self.max_rss_mb = max_rss_mb
        self.tracing = tracing
        self.tracing_endpoint_jaeger = tracing_endpoint_jaeger
        self.cpu_profiler = cpu_profiler
        self.max_buffering_delay = _duration_setting(
            "max_buffering_delay",
            max_buffering_delay,
            "max_buffering_delay_usecs",
            max_buffering_delay_usecs,
            "us",
        )
        self.min_batch_size_records = min_batch_size_records
        self.clock_resolution = _duration_setting(
            "clock_resolution",
            clock_resolution,
            "clock_resolution_usecs",
            clock_resolution_usecs,
            "us",
        )
        self.clock_timezone_offset = clock_timezone_offset
        self.provisioning_timeout = _duration_setting(
            "provisioning_timeout",
            provisioning_timeout,
            "provisioning_timeout_secs",
            provisioning_timeout_secs,
            "s",
        )
        if fault_tolerance_model is not None:
            # An explicit null interval disables periodic checkpoints, so the
            # key is always sent when a model is chosen.
            self.fault_tolerance = {
                "model": str(fault_tolerance_model),
                "checkpoint_interval": _duration_setting(
                    "checkpoint_interval",
                    checkpoint_interval,
                    "checkpoint_interval_secs",
                    checkpoint_interval_secs,
                    "s",
                ),
            }
        if resources is not None:
            self.resources = resources.__dict__
        if storage is not None:
            if isinstance(storage, bool):
                self.storage = storage
            elif isinstance(storage, Storage):
                self.storage = storage.__dict__
            else:
                raise ValueError(f"Unknown value '{storage}' for storage")
        self.dev_tweaks = dev_tweaks
        self.env = env
        self.logging = logging

    @staticmethod
    def default() -> "RuntimeConfig":
        return RuntimeConfig(resources=Resources())

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        """
        Create a :class:`.RuntimeConfig` object from a dictionary.
        """

        conf = cls()
        conf.__dict__ = d
        return conf

    def to_dict(self) -> dict:
        return dict((k, v) for k, v in self.__dict__.items() if v is not None)
