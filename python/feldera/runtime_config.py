import re
import warnings
from typing import Any, Mapping, Optional

from feldera.enums import FaultToleranceModel


_BYTES_PER_UNIT = {
    "": 1,
    "k": 1_000,
    "K": 1_000,
    "M": 1_000_000,
    "G": 1_000_000_000,
    "T": 1_000_000_000_000,
    "P": 1_000_000_000_000_000,
    "E": 1_000_000_000_000_000_000,
    "Ki": 1 << 10,
    "Mi": 1 << 20,
    "Gi": 1 << 30,
    "Ti": 1 << 40,
    "Pi": 1 << 50,
    "Ei": 1 << 60,
}
_SIZE_TERM = re.compile(r"([0-9]*)(?:\.([0-9]*))?(.*)$", re.DOTALL)
_MAX_SIZE_BYTES = 2**64 - 1


def _parse_size_bytes(text: str) -> int:
    """Parses a size string such as ``"16G"``, ``"4Gi"`` or ``"4096"`` into bytes.

    Implements the grammar the pipeline accepts: a number and a unit, decimal
    (``k``, ``M``, ``G``, ``T``, ``P``, ``E``) or binary (``Ki`` through
    ``Ei``), with an optional trailing ``B``; a bare number is a number of
    bytes; one optional leading ``+``. A fraction scales exactly and rounds to
    the nearest byte, as the pipeline does.

    :raises ValueError: if ``text`` is not a size.
    """
    if text.startswith("-"):
        raise ValueError(f"size '{text}' cannot be negative")
    rest = text[1:] if text.startswith("+") else text
    if rest == "":
        raise ValueError("size is empty")
    whole, fraction, unit = _SIZE_TERM.match(rest).groups()
    unit_bytes = _BYTES_PER_UNIT.get(unit[:-1] if unit.endswith("B") else unit)
    if unit_bytes is None:
        raise ValueError(
            f"unknown unit '{unit}' in size '{text}'; expected one of "
            + ", ".join(f"'{u}'" for u in _BYTES_PER_UNIT if u and u != "K")
            + ", each with or without a trailing 'B'"
        )
    if not whole and not fraction:
        if not unit:
            raise ValueError(f"size '{text}' has no digits")
        raise ValueError(f"'{unit}' in size '{text}' has no number in front of it")
    total = int(whole or 0) * unit_bytes
    if fraction:
        fraction = fraction[:_MAX_FRACTION_DIGITS]
        denominator = 10 ** len(fraction)
        total += (unit_bytes * int(fraction) + denominator // 2) // denominator
    if total > _MAX_SIZE_BYTES:
        raise ValueError(f"size '{text}' is too large")
    return total


def _size_setting(
    new_name: str,
    new_value: Optional[str],
    old_name: str,
    old_value: Optional[int],
    unit: str,
    stacklevel: int = 3,
) -> tuple[str, Any]:
    """Resolves a size setting given through either its current name or the
    deprecated integer argument, as the key to send and its value.

    Works as :func:`_duration_setting` does: the setting is sent under the
    spelling the caller used, so code written against the older argument keeps
    working against a server from before the current names, and either use of
    the deprecated argument warns.

    A string with no unit is refused for a setting whose deprecated argument
    counted megabytes, as the server refuses it: ``"16000"`` would be sixteen
    kilobytes where ``16000`` meant sixteen gigabytes.

    :raises ValueError: if ``new_value`` is a bare number and ``unit`` is not
        bytes.
    """
    if isinstance(new_value, str) and unit:
        digits = new_value[1:] if new_value.startswith("+") else new_value
        if re.fullmatch(r"[0-9]+\.?[0-9]*|\.[0-9]+", digits):
            raise ValueError(
                f"{new_name}='{new_value}' has no unit; write a size such as "
                f"'{new_value}{unit}'"
            )
    if old_value is None:
        return new_name, new_value
    equivalent = f"{old_value}{unit}"
    if new_value is None:
        warnings.warn(
            f"'{old_name}' is deprecated; use {new_name}='{equivalent}' instead",
            DeprecationWarning,
            stacklevel=stacklevel,
        )
        return old_name, old_value
    warnings.warn(
        f"'{old_name}' is deprecated and was ignored because "
        f"{new_name}='{new_value}' is also set; drop "
        f"{old_name}={old_value} (it would mean '{equivalent}')",
        DeprecationWarning,
        stacklevel=stacklevel,
    )
    return new_name, new_value


def _size_attribute(new_name: str, old_name: str, unit: str) -> property:
    """A size attribute that reads whichever spelling the configuration holds
    and, when assigned, replaces both."""

    def get(self) -> Optional[str]:
        value = self.__dict__.get(new_name)
        if value is None:
            value = self.__dict__.get(old_name)
        # A bare number under either name counts the old unit, as the
        # server reads it.
        if isinstance(value, int):
            return f"{value}{unit}"
        return value

    def set(self, value: Optional[str]) -> None:
        self.__dict__.pop(old_name, None)
        self.__dict__[new_name] = value

    return property(get, set, doc=f'``{new_name}``, a size such as ``"4Gi"``.')


def _legacy_size_attribute(old_name: str, new_name: str, unit: str) -> property:
    """A deprecated integer attribute of a size setting.

    Reading it converts a size stored under the current name to a whole number
    of the old unit, rounded up as the manager rounds for an older runtime, so
    that a small size written back under the old name never becomes zero;
    assigning it stores the number under the old key, which every server
    reads, in place of the current one."""

    def get(self) -> Optional[int]:
        warnings.warn(
            f"'{old_name}' is deprecated; read '{new_name}' instead",
            DeprecationWarning,
            stacklevel=2,
        )
        legacy = self.__dict__.get(old_name)
        if legacy is not None:
            return legacy
        size = self.__dict__.get(new_name)
        if size is None or not isinstance(size, str):
            return size
        return -(-_parse_size_bytes(size) // _BYTES_PER_UNIT[unit])

    def set(self, value: Optional[int]) -> None:
        warnings.warn(
            f"'{old_name}' is deprecated; set '{new_name}' instead",
            DeprecationWarning,
            stacklevel=2,
        )
        self.__dict__.pop(new_name, None)
        self.__dict__[old_name] = value

    return property(
        get, set, doc=f"Deprecated: ``{new_name}`` as a whole number of the old unit."
    )


def _apply_config(
    target: Any, config: Mapping[str, Any], spellings: tuple[tuple[str, str], ...]
) -> None:
    """Applies ``config`` over the arguments already stored in ``target``.

    ``config`` wins, as it always has, but for the whole setting rather than
    for one key: an argument under either name of a setting in ``spellings``
    is dropped when ``config`` writes that setting under either name. The
    object would otherwise carry both spellings, which the server rejects as
    the setting written twice."""
    for new_name, old_name in spellings:
        if new_name in config or old_name in config:
            target.__dict__.pop(new_name, None)
            target.__dict__.pop(old_name, None)
    target.__dict__.update(config)


class Resources:
    """
    Class used to specify the resource configuration for a pipeline.

    Memory and storage sizes are strings made of a number and a unit, such as
    ``"4Gi"``, ``"16G"`` or ``"400Gi"``. The deprecated integer arguments count
    megabytes and emit a :class:`DeprecationWarning`.

    :param config: A dictionary containing all the configuration values.
    :param cpu_cores_max: The maximum number of CPU cores to reserve for an instance of the pipeline.
    :param cpu_cores_min: The minimum number of CPU cores to reserve for an instance of the pipeline.
    :param memory_max: The maximum memory to reserve for an instance of the pipeline, such as ``"16Gi"``.
    :param memory_min: The minimum memory to reserve for an instance of the pipeline, such as ``"4Gi"``.
    :param storage_class: The storage class to use for the pipeline. The class determines storage performance such
        as IOPS and throughput.
    :param volume_attributes_class: The Kubernetes volume attributes class to use for the pipeline storage. The class
        sets mutable storage parameters such as IOPS and throughput.
    :param storage_max: The storage to reserve for an instance of the pipeline, such as ``"400Gi"``.
    :param storage_min: The initial storage of an instance of the pipeline; set with ``storage_max`` to
        enable storage autoscaling.
    :param memory_mb_max: Deprecated; use ``memory_max``.
    :param memory_mb_min: Deprecated; use ``memory_min``.
    :param storage_mb_max: Deprecated; use ``storage_max``.
    :param storage_mb_min: Deprecated; use ``storage_min``.
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
        memory_max: Optional[str] = None,
        memory_min: Optional[str] = None,
        storage_max: Optional[str] = None,
        storage_min: Optional[str] = None,
        storage_mb_min: Optional[int] = None,
    ):
        config = config or {}

        self.cpu_cores_max = cpu_cores_max
        self.cpu_cores_min = cpu_cores_min
        self._store_size("memory_max", memory_max, "memory_mb_max", memory_mb_max)
        self._store_size("memory_min", memory_min, "memory_mb_min", memory_mb_min)
        self.storage_class = storage_class
        self.volume_attributes_class = volume_attributes_class
        self._store_size("storage_max", storage_max, "storage_mb_max", storage_mb_max)
        self._store_size("storage_min", storage_min, "storage_mb_min", storage_mb_min)

        _apply_config(
            self,
            config,
            (
                ("memory_max", "memory_mb_max"),
                ("memory_min", "memory_mb_min"),
                ("storage_max", "storage_mb_max"),
                ("storage_min", "storage_mb_min"),
            ),
        )

    def _store_size(
        self,
        new_name: str,
        new_value: Optional[str],
        old_name: str,
        old_value: Optional[int],
    ) -> None:
        """Stores a megabyte-counted size setting under `new_name` or
        `old_name`, whichever the caller gave a value for, and nothing when
        neither was given: a `config` read back from an older server carries
        the older key, and a `None` under the current one beside it would be
        rejected by the server as the setting written twice."""
        key, value = _size_setting(
            new_name, new_value, old_name, old_value, "M", stacklevel=4
        )
        if value is not None:
            self.__dict__[key] = value

    memory_max = _size_attribute("memory_max", "memory_mb_max", "M")
    memory_min = _size_attribute("memory_min", "memory_mb_min", "M")
    storage_max = _size_attribute("storage_max", "storage_mb_max", "M")
    storage_min = _size_attribute("storage_min", "storage_mb_min", "M")
    memory_mb_max = _legacy_size_attribute("memory_mb_max", "memory_max", "M")
    memory_mb_min = _legacy_size_attribute("memory_mb_min", "memory_min", "M")
    storage_mb_max = _legacy_size_attribute("storage_mb_max", "storage_max", "M")
    storage_mb_min = _legacy_size_attribute("storage_mb_min", "storage_min", "M")


class Storage:
    """Storage configuration for a pipeline.

    :param min_spill_size: The minimum estimated size of a batch of data to write it to storage,
        such as ``"10Mi"``.
    :param min_storage_bytes: Deprecated; use ``min_spill_size``.
    """

    def __init__(
        self,
        config: Optional[Mapping[str, Any]] = None,
        min_storage_bytes: Optional[int] = None,
        min_spill_size: Optional[str] = None,
    ):
        config = config or {}

        key, value = _size_setting(
            "min_spill_size", min_spill_size, "min_storage_bytes", min_storage_bytes, ""
        )
        if value is not None:
            self.__dict__[key] = value

        _apply_config(self, config, (("min_spill_size", "min_storage_bytes"),))

    min_spill_size = _size_attribute("min_spill_size", "min_storage_bytes", "")
    min_storage_bytes = _legacy_size_attribute(
        "min_storage_bytes", "min_spill_size", ""
    )


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
    ``"500ms"``, ``"30s"``, ``"1h30m"`` or ``"30d"``, and size settings one
    such as ``"16G"``, ``"4Gi"`` or ``"4096"``. The deprecated integer
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
        datafusion_memory: Optional[str] = None,
        max_rss: Optional[str] = None,
    ):
        self.workers = workers
        self.hosts = hosts
        self._store_size(
            "datafusion_memory",
            datafusion_memory,
            "datafusion_memory_mb",
            datafusion_memory_mb,
        )
        self._store_size("max_rss", max_rss, "max_rss_mb", max_rss_mb)
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
        """Stores a duration setting in this object's attributes, under
        `new_name` or `old_name`, whichever the caller gave a value for."""
        key, value = _duration_setting(new_name, new_value, old_name, old_value, unit)
        self.__dict__[key] = value

    def _store_size(
        self,
        new_name: str,
        new_value: Optional[str],
        old_name: str,
        old_value: Optional[int],
    ) -> None:
        """Stores a megabyte-counted size setting under `new_name` or
        `old_name`, whichever the caller gave a value for."""
        key, value = _size_setting(
            new_name, new_value, old_name, old_value, "M", stacklevel=4
        )
        if value is not None:
            self.__dict__[key] = value

    def _legacy_integer(self, old_name: str, new_name: str, unit: str) -> Optional[int]:
        """The value of a deprecated integer attribute, whichever spelling the
        configuration holds. A duration stored under the current name converts
        to a whole number of the old unit, rounded up as the manager rounds
        for an older runtime, so that a short timeout written back under the
        old name never becomes a zero one; a number stored under the old name
        is already in that unit."""
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
        return -(-_parse_duration_nanos(duration) // unit_nanos)

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
    max_rss = _size_attribute("max_rss", "max_rss_mb", "M")
    datafusion_memory = _size_attribute(
        "datafusion_memory", "datafusion_memory_mb", "M"
    )
    max_rss_mb = _legacy_size_attribute("max_rss_mb", "max_rss", "M")
    datafusion_memory_mb = _legacy_size_attribute(
        "datafusion_memory_mb", "datafusion_memory", "M"
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
