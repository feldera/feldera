from enum import Enum
from typing import Optional


class CompilationProfile(Enum):
    """
    The compilation profile to use when compiling the program.
    """

    SERVER_DEFAULT = None
    """
    The compiler server default compilation profile.
    """

    DEV = "dev"
    """
    The development compilation profile.
    """

    UNOPTIMIZED = "unoptimized"
    """
    The unoptimized compilation profile.
    """

    OPTIMIZED = "optimized"
    """
    The optimized compilation profile, the default for this API.
    """


class BuildMode(Enum):
    CREATE = 1
    GET = 2
    GET_OR_CREATE = 3


class PipelineStatus(Enum):
    """
    Represents the state that this pipeline is currently in.

    .. code-block:: text

                    Stopped ◄─────────── Stopping ◄───── All states can transition
                       │                    ▲            to Stopping by either:
      /start or /pause │                    │            (1) user calling /stop?force=true, or;
                       ▼                    │            (2) pipeline encountering a fatal
                ⌛Provisioning          Suspending            resource or runtime error,
                       │                    ▲                having the system call /stop?force=true
                       ▼                    │ /stop          effectively
                ⌛Initializing ─────────────┤  ?force=false
                       │                    │
             ┌─────────┼────────────────────┴─────┐
             │         ▼                          │
             │       Paused  ◄──────► Unavailable │
             │        │   ▲                ▲      │
             │ /start │   │  /pause        │      │
             │        ▼   │                │      │
             │       Running ◄─────────────┘      │
             └────────────────────────────────────┘

    """

    NOT_FOUND = 0
    """
    The pipeline has not been created yet.
    """

    STOPPED = 1
    """
    The pipeline has not (yet) been started or has been stopped either
    manually by the user or automatically by the system due to a
    resource or runtime error.

    The pipeline remains in this state until:

        1. The user starts it via `/start` or `/pause`, transitioning to `PROVISIONING`.
        2. Early start fails (e.g., compilation failure), transitioning to `STOPPING`.
    """

    PROVISIONING = 2
    """
    Compute (and optionally storage) resources needed for running the pipeline
    are being provisioned.

    The pipeline remains in this state until:

        1. Resources are provisioned successfully, transitioning to `INITIALIZING`.
        2. Provisioning fails or times out, transitioning to `STOPPING`.
        3. The user cancels the pipeline via `/stop`, transitioning to `STOPPING`.
    """

    INITIALIZING = 3
    """
    The pipeline is initializing its internal state and connectors.

    The pipeline remains in this state until:

        1. Initialization succeeds, transitioning to `PAUSED`.
        2. Initialization fails or times out, transitioning to `STOPPING`.
        3. The user suspends the pipeline via `/suspend`, transitioning to `SUSPENDING`.
        4. The user stops the pipeline via `/stop`, transitioning to `STOPPING`.
    """

    PAUSED = 4
    """
    The pipeline is initialized but data processing is paused.

    The pipeline remains in this state until:

        1. The user starts it via `/start`, transitioning to `RUNNING`.
        2. A runtime error occurs, transitioning to `STOPPING`.
        3. The user suspends it via `/suspend`, transitioning to `SUSPENDING`.
        4. The user stops it via `/stop`, transitioning to `STOPPING`.
    """

    RUNNING = 5
    """
    The pipeline is processing data.

    The pipeline remains in this state until:

        1. The user pauses it via `/pause`, transitioning to `PAUSED`.
        2. A runtime error occurs, transitioning to `STOPPING`.
        3. The user suspends it via `/suspend`, transitioning to `SUSPENDING`.
        4. The user stops it via `/stop`, transitioning to `STOPPING`.
    """

    UNAVAILABLE = 6
    """
    The pipeline was initialized at least once but is currently unreachable
    or not ready.

    The pipeline remains in this state until:

        1. A successful status check transitions it back to `PAUSED` or `RUNNING`.
        2. A runtime error occurs, transitioning to `STOPPING`.
        3. The user suspends it via `/suspend`, transitioning to `SUSPENDING`.
        4. The user stops it via `/stop`, transitioning to `STOPPING`.

    Note: While in this state, `/start` or `/pause` express desired state but
    are only applied once the pipeline becomes reachable.
    """

    SUSPENDING = 7
    """
    The pipeline is being suspended to storage.

    The pipeline remains in this state until:

        1. Suspension succeeds, transitioning to `STOPPING`.
        2. A runtime error occurs, transitioning to `STOPPING`.
    """

    STOPPING = 8
    """
    The pipeline's compute resources are being scaled down to zero.

    The pipeline remains in this state until deallocation completes,
    transitioning to `STOPPED`.
    """

    @staticmethod
    def from_str(value):
        for member in PipelineStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(f"Unknown value '{value}' for enum {PipelineStatus.__name__}")

    def __eq__(self, other):
        return self.value == other.value


class ProgramStatus(Enum):
    Pending = 1
    CompilingSql = 2
    SqlCompiled = 3
    CompilingRust = 4
    Success = 5
    SqlError = 6
    RustError = 7
    SystemError = 8

    def __init__(self, value):
        self.error: Optional[dict] = None
        self._value_ = value

    @staticmethod
    def from_value(value):
        error = None
        if isinstance(value, dict):
            error = value
            value = list(value.keys())[0]

        for member in ProgramStatus:
            if member.name.lower() == value.lower():
                member.error = error
                return member
        raise ValueError(f"Unknown value '{value}' for enum {ProgramStatus.__name__}")

    def __eq__(self, other):
        return self.value == other.value

    def __str__(self):
        return self.name + (f": ({self.error})" if self.error else "")

    def get_error(self) -> Optional[dict]:
        """
        Returns the compilation error, if any.
        """

        return self.error


class CheckpointStatus(Enum):
    Success = 1
    Failure = 2
    InProgress = 3
    Unknown = 4

    def __init__(self, value):
        self.error: Optional[str] = None
        self._value_ = value

    def __eq__(self, other):
        return self.value == other.value

    def get_error(self) -> Optional[str]:
        """
        Returns the error, if any.
        """

        return self.error


class StorageStatus(Enum):
    """
    Represents the current storage usage status of the pipeline.
    """

    CLEARED = 0
    """
    The pipeline has not been started before, or the user has cleared storage.

    In this state, the pipeline has no storage resources bound to it.
    """

    INUSE = 1
    """
    The pipeline was (attempted to be) started before, transitioning from `STOPPED`
    to `PROVISIONING`, which caused the storage status to become `INUSE`.

    Being in the `INUSE` state restricts certain edits while the pipeline is `STOPPED`.

    The pipeline remains in this state until the user invokes `/clear`, transitioning
    it to `CLEARING`.
    """

    CLEARING = 2
    """
    The pipeline is in the process of becoming unbound from its storage resources.

    If storage resources are configured to be deleted upon clearing, their deletion
    occurs before transitioning to `CLEARED`. Otherwise, no actual work is required,
    and the transition happens immediately.

    If storage is not deleted during clearing, the responsibility to manage or delete
    those resources lies with the user.
    """

    @staticmethod
    def from_str(value):
        for member in StorageStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(f"Unknown value '{value}' for enum {StorageStatus.__name__}")

    def __eq__(self, other):
        return self.value == other.value
