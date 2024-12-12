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

        Shutdown     ◄────┐
        │         │
        /deploy   │       │
        │   ⌛ShuttingDown
        ▼         ▲
        ⌛Provisioning    │
        │         │
        Provisioned        │
        ▼         │/shutdown
        ⌛Initializing     │
        │        │
        ┌────────┴─────────┴─┐
        │        ▼           │
        │      Paused        │
        │      │    ▲        │
        │/start│    │/pause  │
        │      ▼    │        │
        │     Running        │
        └──────────┬─────────┘
                   │
                   ▼
                Failed
    """

    NOT_FOUND = 1
    """
    The pipeline has not been created yet.
    """

    SHUTDOWN = 2
    """
    Pipeline has not been started or has been shut down.

    The pipeline remains in this state until the user triggers
    a deployment by invoking the `/deploy` endpoint.
    """

    PROVISIONING = 3
    """
    The runner triggered a deployment of the pipeline and is
    waiting for the pipeline HTTP server to come up.

    In this state, the runner provisions a runtime for the pipeline,
    starts the pipeline within this runtime and waits for it to start accepting HTTP requests.

    The user is unable to communicate with the pipeline during this
    time.  The pipeline remains in this state until:

        1. Its HTTP server is up and running; the pipeline transitions to the
           `PipelineStatus.INITIALIZING` state.
        2. A pre-defined timeout has passed.  The runner performs forced
           shutdown of the pipeline; returns to the `PipelineStatus.SHUTDOWN` state.
        3. The user cancels the pipeline by invoking the `/shutdown` endpoint.
           The manager performs forced shutdown of the pipeline, returns to the
           `PipelineStatus.SHUTDOWN` state.

    """

    INITIALIZING = 4
    """
    The pipeline is initializing its internal state and connectors.

    This state is part of the pipeline's deployment process.  In this state,
    the pipeline's HTTP server is up and running, but its query engine
    and input and output connectors are still initializing.

    The pipeline remains in this state until:

        1.  Initialization completes successfully; the pipeline transitions to the
            `PipelineStatus.PAUSED` state.
        2.  Initialization fails; transitions to the `PipelineStatus.FAILED` state.
        3.  A pre-defined timeout has passed.  The runner performs forced
            shutdown of the pipeline; returns to the `PipelineStatus.SHUTDOWN` state.
        4.  The user cancels the pipeline by invoking the `/shutdown` endpoint.
            The manager performs forced shutdown of the pipeline; returns to the
            `PipelineStatus.SHUTDOWN` state.

    """

    PAUSED = 5
    """
    The pipeline is fully initialized, but data processing has been paused.

    The pipeline remains in this state until:

        1.  The user starts the pipeline by invoking the `/start` endpoint. The
            manager passes the request to the pipeline; transitions to the
            `PipelineStatus.RUNNING` state.
        2.  The user cancels the pipeline by invoking the `/shutdown` endpoint.
            The manager passes the shutdown request to the pipeline to perform a
            graceful shutdown; transitions to the `PipelineStatus.SHUTTING_DOWN` state.
        3.  An unexpected runtime error renders the pipeline `PipelineStatus.FAILED`.

    """

    RUNNING = 6
    """
    The pipeline is processing data.

    The pipeline remains in this state until:

        1. The user pauses the pipeline by invoking the `/pause` endpoint. The
           manager passes the request to the pipeline; transitions to the
           `PipelineStatus.PAUSED` state.
        2. The user cancels the pipeline by invoking the `/shutdown` endpoint.
           The runner passes the shutdown request to the pipeline to perform a
           graceful shutdown; transitions to the
           `PipelineStatus.SHUTTING_DOWN` state.
        3. An unexpected runtime error renders the pipeline
           `PipelineStatus.FAILED`.

    """

    SHUTTING_DOWN = 7
    """
    Graceful shutdown in progress.

    In this state, the pipeline finishes any ongoing data processing,
    produces final outputs, shuts down input/output connectors and
    terminates.

    The pipeline remains in this state until:

        1. Shutdown completes successfully; transitions to the `PipelineStatus.SHUTDOWN` state.
        2. A pre-defined timeout has passed. The manager performs forced shutdown of the pipeline; returns to the
           `PipelineStatus.SHUTDOWN` state.

    """

    FAILED = 8
    """
    The pipeline remains in this state until the users acknowledge the failure
    by issuing a call to shutdown the pipeline; transitions to the
    `PipelineStatus.SHUTDOWN` state.
    """

    UNAVAILABLE = 9
    """
    The pipeline was at least once initialized, but in the most recent status check either
    could not be reached or returned it is not yet ready.
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
