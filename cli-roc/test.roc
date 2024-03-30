app "hello"
    packages {
        pf: "https://github.com/roc-lang/basic-cli/releases/download/0.7.0/bkGby8jb0tmZYsy2hg1E_B2QrCgcSTxdUlHtETwm5m4.tar.br",
    }
    imports [pf.Task.{ Task, await }, pf.Http, pf.Stdout]
    provides [main] to pf

url = "http://localhost:8080"
main =
    _ <-
        { Http.defaultRequest &
            url: Str.concat url "/config/authentication",
            timeout: TimeoutMilliseconds 5000,
        }
        |> Http.send
        |> Task.attempt
    {} <- Stdout.line "Completed ping request." |> await
    Task.ok {}
