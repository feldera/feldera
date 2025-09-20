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


class DeploymentDesiredStatus(Enum):
    """
    Deployment desired status of the pipeline.
    """

    STOPPED = 0
    UNAVAILABLE = 1
    STANDBY = 2
    PAUSED = 3
    RUNNING = 4
    SUSPENDED = 5

    @staticmethod
    def from_str(value):
        for member in DeploymentDesiredStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(
            f"Unknown value '{value}' for enum {DeploymentDesiredStatus.__name__}"
        )


class DeploymentResourcesDesiredStatus(Enum):
    """
    The desired status of deployment resources of the pipeline.
    """

    STOPPED = 0
    PROVISIONED = 1

    @staticmethod
    def from_str(value):
        for member in DeploymentResourcesDesiredStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(
            f"Unknown value '{value}' for enum {DeploymentResourcesDesiredStatus.__name__}"
        )


class DeploymentResourcesStatus(Enum):
    """
    The desired status of deployment resources of the pipeline.
    """

    STOPPED = 0
    PROVISIONING = 1
    PROVISIONED = 2
    STOPPING = 3

    @staticmethod
    def from_str(value):
        for member in DeploymentResourcesStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(
            f"Unknown value '{value}' for enum {DeploymentResourcesStatus.__name__}"
        )


class DeploymentRuntimeDesiredStatus(Enum):
    """
    Deployment runtime desired status of the pipeline.
    """

    UNAVAILABLE = 0
    STANDBY = 1
    PAUSED = 2
    RUNNING = 3
    SUSPENDED = 4

    @staticmethod
    def from_str(value):
        for member in DeploymentRuntimeDesiredStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(
            f"Unknown value '{value}' for enum {DeploymentRuntimeDesiredStatus.__name__}"
        )


class DeploymentRuntimeStatus(Enum):
    """
    Deployment runtime status of the pipeline.
    """

    UNAVAILABLE = 0
    STANDBY = 1
    INITIALIZING = 2
    BOOTSTRAPPING = 3
    REPLAYING = 4
    PAUSED = 5
    RUNNING = 6
    SUSPENDED = 7

    @staticmethod
    def from_str(value):
        for member in DeploymentRuntimeStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(
            f"Unknown value '{value}' for enum {DeploymentRuntimeStatus.__name__}"
        )


class PipelineStatus(Enum):
    """
    Represents the state that this pipeline is currently in.
    """

    NOT_FOUND = 0
    STOPPED = 1
    PROVISIONING = 2
    UNAVAILABLE = 3
    STANDBY = 4
    INITIALIZING = 5
    BOOTSTRAPPING = 6
    REPLAYING = 7
    PAUSED = 8
    RUNNING = 9
    SUSPENDED = 10
    STOPPING = 11

    @staticmethod
    def from_str(value):
        for member in PipelineStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(f"Unknown value '{value}' for enum {PipelineStatus.__name__}")

    def __eq__(self, other):
        return self.value == other.value


class TransactionStatus(Enum):
    """
    Represents the transaction handling status of a pipeline.
    """

    NoTransaction = 1
    """There is currently no active transaction."""

    TransactionInProgress = 2
    """There is an active transaction in progress."""

    CommitInProgress = 3
    """A commit is currently in progress."""

    @staticmethod
    def from_str(value):
        for member in TransactionStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(
            f"Unknown value '{value}' for enum {TransactionStatus.__name__}"
        )

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


class FaultToleranceModel(Enum):
    """
    The fault tolerance model.
    """

    AtLeastOnce = 1
    """
    Each record is output at least once.  Crashes may duplicate output, but
    no input or output is dropped.
    """

    ExactlyOnce = 2
    """
    Each record is output exactly once.  Crashes do not drop or duplicate
    input or output.
    """

    def __str__(self) -> str:
        match self:
            case FaultToleranceModel.AtLeastOnce:
                return "at_least_once"
            case FaultToleranceModel.ExactlyOnce:
                return "exactly_once"

    @staticmethod
    def from_str(value):
        for member in FaultToleranceModel:
            if str(member) == value.lower():
                return member

        raise ValueError(
            f"Unknown value '{value}' for enum {FaultToleranceModel.__name__}"
        )


class PipelineFieldSelector(Enum):
    ALL = "all"
    """Select all fields of a pipeline."""

    STATUS = "status"
    """Select only the fields required to know the status of a pipeline."""
