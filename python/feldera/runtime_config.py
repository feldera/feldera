from typing import Optional, Any, Mapping
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


class RuntimeConfig:
    """
    Runtime configuration class to define the configuration for a pipeline.
    To create runtime config from a dictionary, use
    :meth:`.RuntimeConfig.from_dict`.

    Documentation:
        https://docs.feldera.com/pipelines/configuration/#runtime-configuration
    """

    def __init__(
        self,
        workers: Optional[int] = None,
        storage: Optional[Storage | bool] = None,
        tracing: Optional[bool] = False,
        tracing_endpoint_jaeger: Optional[str] = "",
        cpu_profiler: bool = True,
        max_buffering_delay_usecs: int = 0,
        min_batch_size_records: int = 0,
        clock_resolution_usecs: Optional[int] = None,
        provisioning_timeout_secs: Optional[int] = None,
        resources: Optional[Resources] = None,
        fault_tolerance_model: Optional[FaultToleranceModel] = None,
        checkpoint_interval_secs: Optional[int] = None,
        dev_tweaks: Optional[dict] = None,
    ):
        self.workers = workers
        self.tracing = tracing
        self.tracing_endpoint_jaeger = tracing_endpoint_jaeger
        self.cpu_profiler = cpu_profiler
        self.max_buffering_delay_usecs = max_buffering_delay_usecs
        self.min_batch_size_records = min_batch_size_records
        self.clock_resolution_usecs = clock_resolution_usecs
        self.provisioning_timeout_secs = provisioning_timeout_secs
        if fault_tolerance_model is not None:
            self.fault_tolerance = {
                "model": str(fault_tolerance_model),
                "checkpoint_interval_secs": checkpoint_interval_secs,
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
