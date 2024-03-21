app "hello"
    packages {
        pf: "https://github.com/roc-lang/basic-cli/releases/download/0.8.1/x8URkvfyi9I0QhmVG98roKBUs_AZRkLFwFJVJ3942YA.tar.br",
        json: "https://github.com/lukewilliamboswell/roc-json/releases/download/0.6.3/_2Dh4Eju2v_tFtZeMq8aZ9qw2outG04NbkmKpFhXS_4.tar.br",
    }
    imports [
        pf.Stdin,
        pf.Stdout,
        pf.Stderr,
        pf.Arg,
        pf.Path,
        pf.File,
        pf.Task.{ Task, await },
        pf.Cmd,
        pf.Env,
        pf.Http,
        pf.Cmd,
        json.Core.{ jsonWithOptions },
        App.Cmd.Workspace.{ workspaceCtx, putWorkspaceCtx, updateWorkspaceDefault, onConflictReplace },
        App.Cmd.Local.{ cmdLocalUpgrade },
        Lib.Some.{ some, mapSome, fromSome, requireSome, fromSomeWith },
        App.Cmd.Local.{ cmdLocal, cmdLocalShutdown },
    ]
    provides [main] to pf

cmdConnectors = \type ->
    when type is
        Some name -> Str.concat "connectors" name |> Stdout.line
        None -> Stdout.line "connectors all"

cmdStop = \pipelines -> Str.joinWith pipelines ", " |> \names -> Str.concat "stop these: " names |> Stdout.line

# bb : [Some {url: Str}, None] -> [Some {url: Str}, None]
# bb = \x -> x

cmdConnect = \url, someName ->
    name =
        when someName is
            Some string -> string
            None -> url
    _ <- {
            url,
            name,
        }
        |> updateWorkspaceDefault "." onConflictReplace
        |> await
    Task.ok {}
# {} <- Stdout.line "ctx2:" |> await
# Inspect.toStr ctx2 |> Stdout.line

cmdWorkspace =
    ctx <- workspaceCtx "." |> await
    when ctx is
        Some { default, known } ->
            _ <- Str.concat "Default deployment - " default.url |> Stdout.line |> await
            Str.concat "Known deployments: \n - " ((List.map known \(name, { url }) -> "$(name): $(url)") |> Str.joinWith "\n - ") |> Stdout.line

        None -> Stdout.line "Feldera workspace not configured"

cmdOpenBrowser =
    ctx <- workspaceCtx "."
        |> await
    when ctx is
        Some { default } ->
            {} <- Stdout.line "UI url: $(default.url)" |> await
            envPath <- Env.var "PATH" |> Task.onErr (\VarNotFound -> crash "PATH env not found, really?") |> await
            # res <-
            #     Cmd.new "xdg-open"
            #     |> Cmd.arg default.url
            #     |> Cmd.env "PATH" envPath
            #     |> Cmd.output
            #     |> Task.onErr (\e -> crash (Inspect.toStr e))
            #     |> await
            # Inspect.toStr res |> Stdout.line
            Task.ok {}

        None -> Stdout.line "Feldera workspace not configured"

main : Task {} *
main =
    rawArgs2 <- Arg.list |> Task.map (\x -> List.dropFirst x 0) |> await
    args2 = List.dropFirst rawArgs2 0
    _ <- args2 |> Inspect.toStr |> Stdout.line |> await

    rawArgs <- Stdin.line
        |> Task.map \l ->
            when l is
                Input s -> s
                End -> crash "No args"
        |> await
    args = rawArgs |> Str.split " "
    when args is
        [] -> Stdout.line "Empty"
        ["--help"] -> Stdout.line "help"
        ["health"] -> Stdout.line "health"
        ["version"] -> Stdout.line "0.0.1"
        ["w"]
        | ["workspace"] -> cmdWorkspace

        ["ps"]
        | ["pipelines"] -> Stdout.line "pipelines"

        ["cs"]
        | ["connectors"] -> cmdConnectors None

        ["cs", name]
        | ["connectors", name] -> cmdConnectors (Some name)

        ["pm"]
        | ["programs"] -> Stdout.line "programs"

        ["pull", name] ->
            when name is
                "all" -> Stdout.line "pull all"
                _ -> Str.concat "pull custom " name |> Stdout.line

        ["push", name] ->
            when name is
                "all" -> Stdout.line "pull all"
                _ -> Str.concat "pull custom " name |> Stdout.line

        ["start", name] -> Str.concat "started " name |> Stdout.line
        ["stop"] -> cmdStop []
        ["stop", name] -> cmdStop [name]
        ["local", "shutdown"] -> cmdLocalShutdown
        ["local", "upgrade"] -> cmdLocalUpgrade
        ["bench", name] -> Str.concat "bench " name |> Stdout.line
        ["ui"] -> cmdOpenBrowser
        ["local"] -> cmdLocal
        [url] -> cmdConnect url None
        [url, name] -> cmdConnect url (Some name)
        _ -> cmdUnknown
# |> Task.onErr \_ -> Stderr.line "Something went wrong!"

cmdUnknown = Stdout.line "help mee"

printHelpMessage = Stdout.line "help"
