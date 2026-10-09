import re
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
    :param volume_attributes_class: The Kubernetes volume attributes class to use for the pipeline storage. The class
        sets mutable storage parameters such as IOPS and throughput.
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
        volume_attributes_class: Optional[str] = None,
        storage_mb_max: Optional[int] = None,
    ):
        config = config or {}

        self.cpu_cores_max = cpu_cores_max
        self.cpu_cores_min = cpu_cores_min
        self.memory_mb_max = memory_mb_max
        self.memory_mb_min = memory_mb_min
        self.storage_class = storage_class
        self.volume_attributes_class = volume_attributes_class
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


_NANOS_PER_UNIT = {
    "ns": 1,
    "us": 1_000,
    "\u00b5s": 1_000,  # micro sign
    "\u03bcs": 1_000,  # Greek small letter mu
    "ms": 1_000_000,
    "s": 1_000_000_000,
    "m": 60 * 1_000_000_000,
    "h": 3_600 * 1_000_000_000,
    "d": 86_400 * 1_000_000_000,
}

# ASCII digits only, as in the runtime's scanner: `\d` alone would also take
# digits from other scripts.
_DURATION_TERM = re.compile(r"(\d*)(?:\.(\d*))?([^\d.]+)", re.ASCII)

# The runtime reads at most this many digits after the point.
_MAX_FRACTION_DIGITS = 18

# The longest duration the runtime holds: `u64::MAX` seconds, plus a second.
_MAX_DURATION_NANOS = (2**64 - 1) * 1_000_000_000 + 999_999_999


def _parse_duration_nanos(text: str) -> int:
    """Parses a duration string such as ``"1h30m"`` into nanoseconds.

    Implements the grammar the pipeline accepts: a run of terms, each a number
    and a unit, summed; a bare ``"0"``; one optional leading ``+``. A fraction
    scales exactly and rounds to the nearest nanosecond, as the pipeline does.

    :raises ValueError: if ``text`` is not a duration.
    """
    if text.startswith("-"):
        raise ValueError(f"duration '{text}' cannot be negative")
    rest = text[1:] if text.startswith("+") else text
    if rest == "":
        raise ValueError("duration is empty")
    if rest == "0":
        return 0
    total = 0
    position = 0
    while position < len(rest):
        match = _DURATION_TERM.match(rest, position)
        if match is None:
            raise ValueError(f"'{rest[position:]}' in duration '{text}' has no unit")
        whole, fraction, unit = match.groups()
        unit_nanos = _NANOS_PER_UNIT.get(unit)
        # Nothing numeric here: a known unit wants a number in front of it,
        # anything else is not a unit at all, as the runtime reports it.
        if not whole and not fraction and unit_nanos is not None:
            raise ValueError(
                f"'{unit}' in duration '{text}' has no number in front of it"
            )
        if unit_nanos is None:
            raise ValueError(
                f"unknown unit '{unit}' in duration '{text}'; expected one of "
                + ", ".join(f"'{u}'" for u in ("ns", "us", "ms", "s", "m", "h", "d"))
            )
        total += int(whole or 0) * unit_nanos
        if fraction:
            fraction = fraction[:_MAX_FRACTION_DIGITS]
            denominator = 10 ** len(fraction)
            total += (unit_nanos * int(fraction) + denominator // 2) // denominator
        position = match.end()
    if total > _MAX_DURATION_NANOS:
        raise ValueError(f"duration '{text}' is too large")
    return total


def _duration_setting(
    new_name: str,
    new_value: Optional[str],
    old_name: str,
    old_value: Optional[int],
    unit: str,
) -> tuple[str, Any]:
    """Resolves a duration setting given through either its current name or the
    deprecated integer argument, as the key to send and its value.

    The setting is sent under the spelling the caller used. A server from
    before the current names accepts only the deprecated key, and one from
    after accepts both, so code written against the older argument keeps
    working against either. The current name wins when both are given. Either
    use of the deprecated argument warns, so that a caller who passes both
    learns that only one of the two took effect.
    """
    if old_value is None:
        return new_name, new_value
    equivalent = f"{old_value}{unit}"
    if new_value is None:
        warnings.warn(
            f"'{old_name}' is deprecated; use {new_name}='{equivalent}' instead",
            DeprecationWarning,
            stacklevel=3,
        )
        return old_name, old_value
    warnings.warn(
        f"'{old_name}' is deprecated and was ignored because "
        f"{new_name}='{new_value}' is also set; drop "
        f"{old_name}={old_value} (it would mean '{equivalent}')",
        DeprecationWarning,
        stacklevel=3,
    )
    return new_name, new_value


def _duration_attribute(new_name: str, old_name: str, unit: str) -> property:
    """A duration attribute that reads whichever spelling the configuration
    holds and, when assigned, replaces both."""

    def get(self) -> Optional[str]:
        value = self.__dict__.get(new_name)
        if value is None:
            legacy = self.__dict__.get(old_name)
            if legacy is not None:
                return f"{legacy}{unit}"
        return value

    def set(self, value: Optional[str]) -> None:
        self.__dict__.pop(old_name, None)
        self.__dict__[new_name] = value

    return property(get, set, doc=f'``{new_name}``, a duration such as ``"30s"``.')


class RuntimeConfig:
    """
    Runtime configuration class to define the configuration for a pipeline.
    To create runtime config from a dictionary, use
    :meth:`.RuntimeConfig.from_dict`.

    Duration settings take a string made of a number and a unit, such as
    ``"500ms"``, ``"30s"``, ``"1h30m"`` or ``"30d"``. The deprecated integer
    arguments and attributes still work but emit a :class:`DeprecationWarning`.

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
        self._store_duration(
            "max_buffering_delay",
            max_buffering_delay,
            "max_buffering_delay_usecs",
            max_buffering_delay_usecs,
            "us",
        )
        self.min_batch_size_records = min_batch_size_records
        self._store_duration(
            "clock_resolution",
            clock_resolution,
            "clock_resolution_usecs",
            clock_resolution_usecs,
            "us",
        )
        self.clock_timezone_offset = clock_timezone_offset
        self._store_duration(
            "provisioning_timeout",
            provisioning_timeout,
            "provisioning_timeout_secs",
            provisioning_timeout_secs,
            "s",
        )
        if fault_tolerance_model is not None:
            # An explicit null interval disables periodic checkpoints, so the
            # key is always sent when a model is chosen.
            key, value = _duration_setting(
                "checkpoint_interval",
                checkpoint_interval,
                "checkpoint_interval_secs",
                checkpoint_interval_secs,
                "s",
            )
            self.fault_tolerance = {"model": str(fault_tolerance_model), key: value}
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

    def _store_duration(
        self,
        new_name: str,
        new_value: Optional[str],
        old_name: str,
        old_value: Optional[int],
        unit: str,
    ) -> None:
        """Stores a duration setting under the spelling it was given in."""
        key, value = _duration_setting(new_name, new_value, old_name, old_value, unit)
        self.__dict__[key] = value

    def _legacy_integer(self, old_name: str, new_name: str, unit: str) -> Optional[int]:
        """The value of a deprecated integer attribute, whichever spelling the
        configuration holds. A duration under the current name converts to a
        whole number of the old unit, rounded to the nearest; a bare number
        under it already counts that unit."""
        warnings.warn(
            f"'{old_name}' is deprecated; read '{new_name}' instead",
            DeprecationWarning,
            stacklevel=3,
        )
        legacy = self.__dict__.get(old_name)
        if legacy is not None:
            return legacy
        duration = self.__dict__.get(new_name)
        if duration is None or not isinstance(duration, str):
            return duration
        unit_nanos = _NANOS_PER_UNIT[unit]
        return (_parse_duration_nanos(duration) + unit_nanos // 2) // unit_nanos

    def _set_legacy_integer(
        self, old_name: str, new_name: str, unit: str, value: Optional[int]
    ) -> None:
        """Stores a deprecated integer attribute under its own key, which every
        server reads, in place of the current one."""
        warnings.warn(
            f"'{old_name}' is deprecated; set '{new_name}' instead",
            DeprecationWarning,
            stacklevel=3,
        )
        self.__dict__.pop(new_name, None)
        self.__dict__[old_name] = value

    max_buffering_delay = _duration_attribute(
        "max_buffering_delay", "max_buffering_delay_usecs", "us"
    )
    clock_resolution = _duration_attribute(
        "clock_resolution", "clock_resolution_usecs", "us"
    )
    provisioning_timeout = _duration_attribute(
        "provisioning_timeout", "provisioning_timeout_secs", "s"
    )

    @property
    def max_buffering_delay_usecs(self) -> Optional[int]:
        """Deprecated: ``max_buffering_delay`` as a whole number of microseconds."""
        return self._legacy_integer(
            "max_buffering_delay_usecs", "max_buffering_delay", "us"
        )

    @max_buffering_delay_usecs.setter
    def max_buffering_delay_usecs(self, value: Optional[int]) -> None:
        self._set_legacy_integer(
            "max_buffering_delay_usecs", "max_buffering_delay", "us", value
        )

    @property
    def clock_resolution_usecs(self) -> Optional[int]:
        """Deprecated: ``clock_resolution`` as a whole number of microseconds."""
        return self._legacy_integer("clock_resolution_usecs", "clock_resolution", "us")

    @clock_resolution_usecs.setter
    def clock_resolution_usecs(self, value: Optional[int]) -> None:
        self._set_legacy_integer(
            "clock_resolution_usecs", "clock_resolution", "us", value
        )

    @property
    def provisioning_timeout_secs(self) -> Optional[int]:
        """Deprecated: ``provisioning_timeout`` as a whole number of seconds."""
        return self._legacy_integer(
            "provisioning_timeout_secs", "provisioning_timeout", "s"
        )

    @provisioning_timeout_secs.setter
    def provisioning_timeout_secs(self, value: Optional[int]) -> None:
        self._set_legacy_integer(
            "provisioning_timeout_secs", "provisioning_timeout", "s", value
        )

    @staticmethod
    def default() -> "RuntimeConfig":
        return RuntimeConfig(resources=Resources())

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]):
        """
        Create a :class:`.RuntimeConfig` object from a dictionary.

        The dictionary is kept as it is, so writing it back sends each setting
        in the spelling it was read in. A duration attribute reads whichever
        spelling is present, and assigning one replaces both.
        """

        conf = cls()
        conf.__dict__ = dict(d)
        return conf

    def to_dict(self) -> dict:
        return dict((k, v) for k, v in self.__dict__.items() if v is not None)
