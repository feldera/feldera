interface Lib.FelderaAPI.Types
    exposes [ResourceConfig, RuntimeConfig, AttachedConnector, PipelineDescr, PipelineRuntimeState, Pipeline, PipelineStatus, ErrorResponse, pipelineStatus]
    imports [
        Lib.Nullable.{ Nullable },
        Lib.Show.{ Show, show },
    ]

# ==============================================================================

# ==============================================================================

ErrorResponse : {
    ##
    ## Detailed error metadata.
    ## The contents of this field is determined by `error_code`.
    ##
    details : {},
    ##
    ## Error code is a string that specifies this error type.
    ##
    errorCode : Str,
    ##
    ## Human-readable error message.
    ##
    message : Str,
}

Version : U32

PipelineId : Str

ResourceConfig : {
    ##
    ## The maximum number of CPU cores to reserve
    ## for an instance of this pipeline
    ##
    cpuCoresMax : Nullable I32,
    ##
    ## The minimum number of CPU cores to reserve
    ## for an instance of this pipeline
    ##
    cpuCoresMin : Nullable I32,
    ##
    ## The maximum memory in Megabytes to reserve
    ## for an instance of this pipeline
    ##
    memoryMbMax : Nullable I32,
    ##
    ## The minimum memory in Megabytes to reserve
    ## for an instance of this pipeline
    ##
    memoryMbMin : Nullable I32,
    ##
    ## The total storage in Megabytes to reserve
    ## for an instance of this pipeline
    ##
    storageMbMax : Nullable I32,
}

RuntimeConfig : {
    ##
    ## Enable CPU profiler.
    ##
    cpuProfiler : Bool,
    ##
    ## Maximal delay in microseconds to wait for `min_batch_size_records` to
    ## get buffered by the controller, defaults to 0.
    ##
    maxBufferingDelayUsecs : U32,
    ##
    ## Minimal input batch size.
    ##
    ## The controller delays pushing input records to the circuit until at
    ## least `min_batch_size_records` records have been received (total
    ## across all endpoints) or `max_buffering_delay_usecs` microseconds
    ## have passed since at least one input records has been buffered.
    ## Defaults to 0.
    ##
    minBatchSizeRecords : U32,
    resources : ResourceConfig,
    ##
    ## Number of DBSP worker threads.
    ##
    workers : U32,
}

AttachedConnector : {
    ##
    ## The name of the connector to attach.
    ##
    connectorName : Str,
    ##
    ## True for input connectors, false for output connectors.
    ##
    isInput : Bool,
    ##
    ## A unique identifier for this attachement.
    ##
    name : Str,
    ##
    ## The table or view this connector is attached to. Unquoted
    ## table/view names in the SQL program need to be capitalized
    ## here. Quoted table/view names have to exactly match the
    ## casing from the SQL program.
    ##
    relationName : Str,
}

PipelineDescr : {
    attachedConnectors : List AttachedConnector,
    config : RuntimeConfig,
    description : Str,
    name : Str,
    pipelineId : PipelineId,
    programName : Str,
    version : Version,
}

PipelineStatus := [
    Shutdown,
    Provisioning,
    Initializing,
    Paused,
    Running,
    ShuttingDown,
    Failed,
]
    implements [
        Decoding { decoder: decodePipelineStatus },
        Encoding, # auto derive
        Inspect, # auto derive
        Eq, # auto derive
        Show { show: showPipelineStatus },
    ]

valuesPipelineStatus = [
    (Shutdown, "Shutdown"),
    (Provisioning, "Provisioning"),
    (Initializing, "Initializing"),
    (Paused, "Paused"),
    (Running, "Running"),
    (ShuttingDown, "ShuttingDown"),
    (Failed, "Failed"),
]

pipelineStatus = \x -> @PipelineStatus x
unPipelineStatus = \@PipelineStatus status -> status
showPipelineStatus = \@PipelineStatus enum ->
    when enum is
        Shutdown -> "Shutdown"
        Provisioning -> "Provisioning"
        Initializing -> "Initializing"
        Paused -> "Paused"
        Running -> "Running"
        ShuttingDown -> "ShuttingDown"
        Failed -> "Failed"

decodePipelineStatus = Decode.custom \bytes, fmt ->
    when Str.fromUtf8 bytes is
        Err err -> crash (Inspect.toStr err)
        Ok enum ->
            when enum is
                _ if Str.startsWith enum "\"Shutdown\"" ->
                    { result: Ok (@PipelineStatus Shutdown), rest: List.dropFirst bytes 10 }

                _ if Str.startsWith enum "\"Provisioning\"" ->
                    { result: Ok (@PipelineStatus Provisioning), rest: List.dropFirst bytes 12 }

                _ if Str.startsWith enum "\"Initializing\"" ->
                    { result: Ok (@PipelineStatus Initializing), rest: List.dropFirst bytes 12 }

                _ if Str.startsWith enum "\"Paused\"" ->
                    { result: Ok (@PipelineStatus Paused), rest: List.dropFirst bytes 6 }

                _ if Str.startsWith enum "\"Running\"" ->
                    { result: Ok (@PipelineStatus Running), rest: List.dropFirst bytes 7 }

                _ if Str.startsWith enum "\"ShuttingDown\"" ->
                    { result: Ok (@PipelineStatus ShuttingDown), rest: List.dropFirst bytes 12 }

                _ if Str.startsWith enum "\"Failed\"" ->
                    { result: Ok (@PipelineStatus Failed), rest: List.dropFirst bytes 6 }

                _ -> crash (Str.concat "Unexpected value for enum PipelineStatus: " enum)

PipelineRuntimeState : {
    ##
    ## Time when the pipeline started executing.
    ##
    created : Str,
    currentStatus : PipelineStatus,
    desiredStatus : PipelineStatus,
    error : Nullable ErrorResponse,
    ##
    ## Location where the pipeline can be reached at runtime.
    ## e.g., a TCP port number or a URI.
    ##
    location : Str,
    pipelineId : PipelineId,
    ##
    ## Time when the pipeline was assigned its current status
    ## of the pipeline.
    ##
    statusSince : Str,
}

Pipeline : {
    descriptor : PipelineDescr,
    state : PipelineRuntimeState,
}
