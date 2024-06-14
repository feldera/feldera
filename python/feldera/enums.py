from enum import Enum


# https://stackoverflow.com/questions/50473951/how-can-i-attach-documentation-to-members-of-a-python-enum/50473952#50473952
class DocEnum(Enum):
    """
    :meta private:
    """

    def __new__(cls, value, doc=None):
        self = object.__new__(cls)  # calling super().__new__(value) here would fail
        self._value_ = value
        if doc is not None:
            self.__doc__ = doc
        return self


class CompilationProfile(DocEnum):
    """
    The compilation profile to use when compiling the program.
    """

    SERVER_DEFAULT = None, "The compiler server default compilation profile."

    DEV = "dev", "The development compilation profile."

    UNOPTIMIZED = "unoptimized", "The unoptimized compilation profile."

    OPTIMIZED = "optimized", "The optimized compilation profile, the default for this API."


class BuildMode(DocEnum):
    CREATE = 1
    GET = 2
    GET_OR_CREATE = 3


class PipelineStatus(DocEnum):
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

    UNINITIALIZED = 1, """
    The pipeline has not been created yet.
    """

    SHUTDOWN = 2, """
    Pipeline has not been started or has been shut down.
    
    The pipeline remains in this state until the user triggers
    a deployment by invoking the `/deploy` endpoint.
    """
    
    PROVISIONING = 3, """
    The runner triggered a deployment of the pipeline and is
    waiting for the pipeline HTTP server to come up.
    
    In this state, the runner provisions a runtime for the pipeline,
    starts the pipeline within this runtime and waits for it to start accepting HTTP requests.
    
    The user is unable to communicate with the pipeline during this
    time.  The pipeline remains in this state until:
    
        1. Its HTTP server is up and running; the pipeline transitions to the
           :py:enum:mem:`PipelineStatus.INITIALIZING` state.
        2. A pre-defined timeout has passed.  The runner performs forced
           shutdown of the pipeline; returns to the :py:enum:mem:`PipelineStatus.SHUTDOWN` state.
        3. The user cancels the pipeline by invoking the `/shutdown` endpoint.
           The manager performs forced shutdown of the pipeline, returns to the
           :py:enum:mem:`PipelineStatus.SHUTDOWN` state.
    
    """

    INITIALIZING = 4, """
    The pipeline is initializing its internal state and connectors.
    
    This state is part of the pipeline's deployment process.  In this state,
    the pipeline's HTTP server is up and running, but its query engine
    and input and output connectors are still initializing.
     
    The pipeline remains in this state until:
    
        1.  Initialization completes successfully; the pipeline transitions to the
            :py:enum:mem:`PipelineStatus.PAUSED` state.
        2.  Initialization fails; transitions to the :py:enum:mem:`PipelineStatus.FAILED` state.
        3.  A pre-defined timeout has passed.  The runner performs forced 
            shutdown of the pipeline; returns to the :py:enum:mem:`PipelineStatus.SHUTDOWN` state.
        4.  The user cancels the pipeline by invoking the `/shutdown` endpoint. 
            The manager performs forced shutdown of the pipeline; returns to the 
            :py:enum:mem:`PipelineStatus.SHUTDOWN` state.
    
    """

    PAUSED = 5, """
    The pipeline is fully initialized, but data processing has been paused.
    
    The pipeline remains in this state until:
    
        1.  The user starts the pipeline by invoking the `/start` endpoint. The
            manager passes the request to the pipeline; transitions to the
            :py:enum:mem:`PipelineStatus.RUNNING` state.
        2.  The user cancels the pipeline by invoking the `/shutdown` endpoint.
            The manager passes the shutdown request to the pipeline to perform a
            graceful shutdown; transitions to the :py:enum:mem:`PipelineStatus.SHUTTING_DOWN` state.
        3.  An unexpected runtime error renders the pipeline :py:enum:mem:`PipelineStatus.FAILED`.
    
    """

    RUNNING = 6, """
    The pipeline is processing data.
    
    The pipeline remains in this state until:
    
        1. The user pauses the pipeline by invoking the `/pause` endpoint. The
           manager passes the request to the pipeline; transitions to the
           :py:enum:mem:`PipelineStatus.PAUSED` state.
        2. The user cancels the pipeline by invoking the `/shutdown` endpoint.
           The runner passes the shutdown request to the pipeline to perform a
           graceful shutdown; transitions to the
           :py:enum:mem:`PipelineStatus.SHUTTING_DOWN` state.
        3. An unexpected runtime error renders the pipeline
           :py:enum:mem:`PipelineStatus.FAILED`.
    
    """
    
    SHUTTING_DOWN = 7, """
    Graceful shutdown in progress.
    
    In this state, the pipeline finishes any ongoing data processing,
    produces final outputs, shuts down input/output connectors and
    terminates.
    
    The pipeline remains in this state until:
    
        1. Shutdown completes successfully; transitions to the :py:enum:mem:`PipelineStatus.SHUTDOWN` state.
        2. A pre-defined timeout has passed. The manager performs forced shutdown of the pipeline; returns to the
           :py:enum:mem:`PipelineStatus.SHUTDOWN` state.
    
    """

    FAILED = 8, """
    The pipeline remains in this state until the users acknowledge the failure
    by issuing a call to shutdown the pipeline; transitions to the
    :py:enum:mem:`PipelineStatus.SHUTDOWN` state.
    """

    @staticmethod
    def from_str(value):
        for member in PipelineStatus:
            if member.name.lower() == value.lower():
                return member
        raise ValueError(f"Unknown value '{value}' for enum {PipelineStatus.__name__}")
