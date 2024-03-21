interface App.Cmd.Local
    exposes [cmdLocal, cmdLocalShutdown, cmdLocalUpgrade]
    imports [
        pf.Task.{ Task, await },
        pf.Dir,
        pf.Stdout,
        pf.Path,
        pf.File,
        pf.Http,
        pf.Cmd,
        json.Core.{ jsonWithOptions },
        Lib.Workspace.{ workspaceCtx, updateDefaultWorkspace, onConflictReplace },
        Lib.Some.{ some, mapSome, fromSome, requireSome },
        Lib.Path.{ pathDirectory },
        Lib.Task.{ returnAfter },
    ]

pingFeldera = \url ->
    res <-
        { Http.defaultRequest &
            url: Str.concat url "/config/authentication",
            timeout: TimeoutMilliseconds 5000,
        }
        |> Http.send
        |> Task.attempt
    {} <- Stdout.line "Completed ping request." |> await
    when res is
        Ok _ -> Stdout.line "Feldera ping success at $(url)!" |> Task.map (\_ -> Bool.true)
        Err _ -> Stdout.line "Failed to ping Feldera deployment!" |> Task.map (\_ -> Bool.false)

pingFelderaWorkaround = \url ->
    res <- Cmd.new "curl"
        |> Cmd.arg "-I"
        |> Cmd.arg "--connect-timeout"
        |> Cmd.arg "5.0" # 5000 ms
        |> Cmd.arg url
        |> Cmd.status
        |> Task.attempt
    when res is
        Ok _ -> Stdout.line "Feldera ping success at $(url)!" |> Task.map (\_ -> Bool.true)
        Err _ -> Stdout.line "Failed to ping Feldera deployment!" |> Task.map (\_ -> Bool.false)

cmdLocalShutdown =
    {} <- Cmd.new "docker"
        |> Cmd.arg "compose"
        |> Cmd.arg "-f"
        |> Cmd.arg localComposeFile
        |> Cmd.arg "--profile"
        |> Cmd.arg "demo"
        |> Cmd.arg "down"
        |> Cmd.status
        |> Task.onErr (\e -> crash (Inspect.toStr e))
        |> await
    Stdout.line "Stopped local Feldera instance"

## Set local deplolyment as default, and start feldera locally.
## The latest Feldera image is downloaded and installed if it wasn't previously.
## Already running local deployment is used as-is.
## The URL of local deployment is expected to be http://localhost:8080.
cmdLocal =
    x <- checkDockerInstalled |> await
    {} <-
        when x is
            NotDetected ->
                returnAfter
                    (
                        Stdout.line "Docker not detected!"
                    )

            Installed ->
                \t ->
                    {} <- Stdout.line "Docker is installed!" |> await
                    t {}
    {} <- Stdout.line "Checking Feldera deployment..." |> await
    validDeployment <- pingFeldera "http://localhost:8080" |> await
    {} <-
        (
            if
                validDeployment
            then
                Task.ok {}
            else
                exists <- composeFileExists |> await
                if
                    exists
                then
                    _ <- Stdout.line "Compose file exists!" |> await
                    Task.ok {}
                else
                    downloadFelderaImage
        )
        |> await

    {} <- { name: "local", url: "http://localhost:8080" } |> updateDefaultWorkspace "." onConflictReplace |> await
    {} <- startupLocalDeployment |> await
    Task.ok {}

localComposeFile = "./local/docker-compose.yml"

composeFileExists =
    # Cmd.new "[ -e /path/to/your/file.txt ] && echo \"0\" || echo \"1\""
    #   |> Cmd.output
    #   |> task.map \{output} -> when output is
    #     "0" -> true
    #     "1" -> false
    Cmd.new "stat"
    |> Cmd.arg localComposeFile
    |> Cmd.output
    |> Task.map \_ -> Bool.true
    |> Task.onErr \_ -> Task.ok Bool.false

# curl -O https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml --output ./local/docker-compose.yml
downloadFelderaImage =
    parentDirectory = Path.fromStr localComposeFile |> pathDirectory
    {} <- "docker-compose dir: $(parentDirectory)" |> Stdout.line |> await
    {} <- Cmd.new "mkdir"
        |> Cmd.arg "-p"
        |> Cmd.arg parentDirectory
        |> Cmd.output
        |> Task.onErr (\e -> crash "crasha $(Inspect.toStr e)")
        |> await
    {} <- Cmd.new "curl"
        |> Cmd.arg "--output"
        |> Cmd.arg localComposeFile
        |> Cmd.arg "https://raw.githubusercontent.com/feldera/feldera/main/deploy/docker-compose.yml"
        |> Cmd.output
        |> Task.onErr (\e -> crash "crashb $(Inspect.toStr e)")
        |> await
    {} <- Stdout.line "Downloaded feldera docker-compose.yml" |> await
    Task.ok {}

# check if docker compose was already run (https://serverfault.com/a/935674):  docker compose -f ./local/docker-compose.yml ps -q pipeline-manager
# docker compose -f ./local/docker-compose.yml up
# docker compose -f ./local/docker-compose.yml --profile demo up
startupLocalDeployment =
    {} <- Stdout.line "Starting Feldera via $(localComposeFile)" |> await
    alreadyRun <- Cmd.new "docker"
        |> Cmd.arg "compose"
        |> Cmd.arg "-f"
        |> Cmd.arg localComposeFile
        |> Cmd.arg "ps"
        |> Cmd.arg "-q"
        |> Cmd.arg "pipeline-manager"
        |> Cmd.output
        |> Task.map \{ stderr, stdout } -> Bool.false
        |> Task.onErr
            (\(output, e) ->
                {} <- Stdout.line (Str.fromUtf8 output.stderr |> Inspect.toStr) |> await
                crash (Inspect.toStr e))
        |> await
    {} <- Cmd.new "docker"
        |> Cmd.arg "compose"
        |> Cmd.arg "-f"
        |> Cmd.arg localComposeFile
        |> (\cmd -> if
                alreadyRun
            then
                cmd |> Cmd.arg "up"
            else
                cmd |> Cmd.arg "--profile" |> Cmd.arg "demo" |> Cmd.arg "up"
        ) # |> Cmd.output
        # |> Task.onErr (\e -> crash (Inspect.toStr e))
        # |> await
        |> Cmd.status
        |> Task.onErr (\e -> crash (Inspect.toStr e))
        |> await
    "Started local docker instance" |> Stdout.line

# docker compose -f ./local/docker-compose.yml --profile demo down
# docker compose -f ./local/docker-compose.yml --profile demo up --pull always --force-recreate -V
cmdLocalUpgrade =
    {} <- Cmd.new "docker"
        |> Cmd.arg "compose"
        |> Cmd.arg "-f"
        |> Cmd.arg localComposeFile
        |> Cmd.arg "--profile"
        |> Cmd.arg "demo"
        |> Cmd.arg "down"
        |> Cmd.output
        |> Task.onErr (\e -> crash (Inspect.toStr e))
        |> await
    {} <- Cmd.new "docker"
        |> Cmd.arg "compose"
        |> Cmd.arg "-f"
        |> Cmd.arg localComposeFile
        |> Cmd.arg "--profile"
        |> Cmd.arg "demo"
        |> Cmd.arg "up"
        |> Cmd.arg "--pull"
        |> Cmd.arg "always"
        |> Cmd.arg "--force-recreate"
        |> Cmd.arg "-V"
        |> Cmd.output
        |> Task.onErr (\e -> crash (Inspect.toStr e))
        |> await
    Task.ok {}

# docker version
# docker compose version
checkDockerInstalled : Task [Installed, NotDetected] *
checkDockerInstalled =
    task =
        {} <- Cmd.new "docker"
            |> Cmd.arg "version"
            |> Cmd.output
            |> await
        Cmd.new "docker"
        |> Cmd.arg "compose"
        |> Cmd.arg "version"
        |> Cmd.output
    # Task.ok Installed |> Task.onErr \_ -> Task.ok NotDetected
    task |> Task.map (\_ -> Installed) |> Task.onErr \_ -> Task.ok NotDetected
