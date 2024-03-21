interface Lib.Workspace
    exposes [WorkspaceData, workspaceCtx, putWorkspaceCtx, updateDefaultWorkspace, onConflictReplace, onConflictSkip, onConflictError, requireDefaultWorkspace, suggestWorkspaceName]
    imports [
        pf.Task.{ Task, await },
        pf.Dir,
        pf.Stdout,
        pf.Path,
        pf.File,
        json.Core.{ jsonWithOptions },
        Lib.Some.{ some, mapSome, fromSome, requireSome },
        Lib.List.{ nubFirstOn },
    ]

json = jsonWithOptions { fieldNameMapping: CamelCase }

WorkspaceData : { default : { name : Str }, known : Dict Str { url : Str } }

# TODO: implement Decoding ability for Dict

felderaJsonPath = \absoluteDir -> Str.concat absoluteDir "/feldera.json" |> Path.fromStr

requireDefaultWorkspace = \absoluteDir ->
    someCtx <- workspaceCtx "." |> await
    when someCtx is
        None -> Task.err (ConfigurationError "Default workspace not configured")
        Some ctx ->
            when Dict.get ctx.known ctx.default.name is
                Err _ ->
                    Task.err (ConfigurationError "Default workspace not known")

                Ok workspace -> Task.ok { url: workspace.url }

## Get the default and known Feldera deployments
workspaceCtx : Str -> Task [Some WorkspaceData, None]a *
workspaceCtx = \absoluteDir ->
    task =
        bytes <- felderaJsonPath absoluteDir |> File.readUtf8 |> Task.map Str.toUtf8 |> await
        # ctx : [None, Some WorkspaceData]
        ctx =
            Decode.fromBytesPartial bytes json
            |> .result # |> decorateDefault
            |> Result.map \{ default, known } -> Some { default, known: Dict.fromList known }
            |> Result.withDefault None
        Task.ok ctx
    task |> Task.onErr (\_ -> Task.ok None)

onConflictReplace = \_old, new -> Ok new
onConflictSkip = \old, _new -> Ok old
onConflictError = \_old, _new -> Err {}

## Store url as known, set passed name as default, resolve name conflict if the same name exists
## Known workspaces with the same url but a different name are kept as-is
# updateDefaultWorkspace : { url : Str, name : Str }, Str, (a, a -> Result a *) -> Task WorkspaceData *
updateDefaultWorkspace = \{ url, name }, absoluteDir, onNameConflict ->
    someCtx <- workspaceCtx absoluteDir |> await
    default = someCtx |> some { name } (.default)

    knownWithResolvedConflicts =
        someCtx
        |> some (Dict.empty {}) .known # |> List.mapTry
        #     \workspace ->
        #         # Replace all known workspaces with matching name using conflict resolution strategy
        #         (n, _) = workspace
        #         if
        #             n == name
        #         then
        #         onNameConflict workspace (name, { url: url })
        #         else if
        #             # If found unnamed workspace with a matching URL
        #             n == url
        #         then
        #             # Name it with a given name
        #             Ok (name, { url: url })
        #         else
        #             Ok workspace
        # |> \result ->
        #     when result is
        #         Ok list -> Dict.insert list (name, { url })
        #         Err _ -> crash "updateDefaultWorkspace onConflict error"
        |> Dict.update name \current ->
            when current is
                Present workspace ->
                    when onNameConflict { url: workspace.url } { url } is
                        Ok value -> Present value
                        Err _ -> crash "updateDefaultWorkspace onConflict error"

                Missing -> Present { url }

    ctx = {
        default,
        known: knownWithResolvedConflicts,
    }

    putWorkspaceCtx absoluteDir ctx

putWorkspaceCtx : Str, WorkspaceData -> Task WorkspaceData *
putWorkspaceCtx = \absoluteDir, ctx ->
    s = Encode.toBytes { default: ctx.default, known: Dict.toList ctx.known } json |> Str.fromUtf8 |> Result.withDefault "{ error: \"Utf error\" }"
    {} <- File.writeUtf8 (felderaJsonPath absoluteDir) s
        |> Task.onErr \err ->
            when err is
                FileWriteErr _path e -> crash (Inspect.toStr e)
        |> await
    Task.ok ctx

suggestWorkspaceName : Str -> Result Str {}
suggestWorkspaceName = \url ->
    (
        body <- Str.split url "//" |> List.get 1 |> Result.try
        path = Str.split body "/"
        origin <- path |> List.get 0 |> Result.map
        originParts = Str.split origin "."
        List.takeFirst originParts (Num.max 1 (List.len originParts - 1)) |> Str.joinWith "-" |> Str.replaceFirst ":" "-"
    )
    |> Result.mapErr \_ -> {}

expect
    suggestWorkspaceName "https://try.feldera.com" == Ok "try-feldera"
# suggestWorkspaceName "https://try.feldera.com" |> Result.map (\x -> x == "try-feldera") |> Result.withDefault Bool.false

expect
    suggestWorkspaceName "https://cloud.feldera.com" == Ok "cloud-feldera"
# suggestWorkspaceName "http://cloud.feldera.com" |> Result.map (\x -> x == "cloud-feldera") |> Result.withDefault Bool.false

expect
    suggestWorkspaceName "http://localhost:8080" == Ok "localhost-8080"
# suggestWorkspaceName "http://localhost:8080" |> Result.map (\x -> x == "localhost") |> Result.withDefault Bool.false

expect
    suggestWorkspaceName "http://localhost:8081" == Ok "localhost-8081"
