interface App.Cmd.Workspace
    exposes [cmdSetDefaultWorkspace]
    imports [
        pf.Task.{ Task, await },
        pf.Dir,
        pf.Stdout,
        pf.Path,
        pf.File,
        Lib.Some.{ some, mapSome, fromSome, requireSome },
        Lib.List.{ nubFirstOn },
        Lib.Workspace.{ workspaceCtx, putWorkspaceCtx, updateDefaultWorkspace, onConflictReplace, onConflictSkip, onConflictError, suggestWorkspaceName },
    ]

cmdSetDefaultWorkspace = \new ->
    when new is
        Url url ->
            when suggestWorkspaceName url is
                Err _ -> crash "Couldn't come up with a name for the workspace"
                Ok name ->
                    _ <- updateDefaultWorkspace { url, name } "." onConflictReplace |> await
                    Task.ok {}

        Known name ->
            someCtx <- workspaceCtx "." |> await
            known : Dict Str { url : Str }
            known = some someCtx (Dict.empty {}) .known
            if
                known |> Dict.contains name
            then
                _ <- putWorkspaceCtx "." { known, default: { name } } |> await
                Task.ok {}
            else
                Stdout.line "Unknown workspace: $(name)"

        Named name url ->
            _ <- (updateDefaultWorkspace { url, name } "." onConflictReplace) |> await
            Task.ok {}
