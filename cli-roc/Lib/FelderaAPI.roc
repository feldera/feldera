interface Lib.FelderaAPI
    exposes [listPipelines]
    imports [
        pf.Task.{ Task, await, Error },
        pf.Dir,
        pf.Stdout,
        pf.Path,
        pf.File,
        pf.Http,
        pf.Cmd,
        json.Core.{ jsonWithOptions },
        Lib.FelderaAPI.Types.{ Pipeline, ErrorResponse, PipelineStatus, pipelineStatus },
        Lib.Nullable.{ Nullable, null, nullableDecode, nullableEncode },
    ]

json = jsonWithOptions { fieldNameMapping: SnakeCase }

listPipelines = \origin ->
    # Task.ok {}
    body <-
        { Http.defaultRequest &
            url: Str.concat origin "/v0/pipelines",
        }
        |> Http.send
        |> Task.onErr
            (\err -> # Task.ok "")
                crash (Inspect.toStr err))
        |> await

    pipelines : DecodeResult (List Pipeline)
    pipelines = body |> Str.toUtf8 |> Decode.fromBytesPartial json
    when pipelines.result is
        Ok ppl -> Task.ok ppl
        Err err -> crash (Inspect.toStr err)

expect
    actual : DecodeResult PipelineStatus
    actual = "\"Shutdown\"" |> Str.toUtf8 |> Decode.fromBytesPartial json
    expected = Ok (pipelineStatus Shutdown)
    actual.result == expected
