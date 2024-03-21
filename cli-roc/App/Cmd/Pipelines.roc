interface App.Cmd.Pipelines
    exposes [cmdListPipelines]
    imports [
        pf.Task.{ Task, await },
        pf.Dir,
        pf.Stdout,
        pf.Path,
        pf.File,
        pf.Http,
        pf.Cmd,
        json.Core.{ json },
        Lib.Nullable.{ Nullable, null },
        Lib.FelderaAPI.{ listPipelines },
        Lib.FelderaAPI.Types.{ Pipeline, ErrorResponse, PipelineStatus, pipelineStatus },
        Lib.Show.{ Show, show },
        Lib.Workspace.{ requireDefaultWorkspace },
    ]

cmdListPipelines = # Stdout.line "programs"
    workspace <- requireDefaultWorkspace "." |> await
    ps <- listPipelines workspace.url |> await
    if
        List.len ps == 0
    then
        Stdout.line "No pipelines in Feldera instance"
    else
        Task.forEach
            ps
            (\p ->
                Stdout.line "$(p.descriptor.name) $(show p.state.currentStatus)")

