interface Lib.Task
    exposes [earlyReturn, returnAfter]
    imports [pf.Task.{ Task, await }, pf.Stdout]

## Prevent the following task from executing with a successful outcome
## Usage:
## {} <-
##     when x is
##         Break ->
##             \t ->
##                 {} <- Stdout.line "Break!" |> await
##                 earlyReturn t
##         Continue ->
##             \t ->
##                 {} <- Stdout.line "Continue!" |> await
##                 t {}
## Stdout.line "Continued!"
earlyReturn : Task {} * -> Task {} *
earlyReturn = \_task ->
    Stdout.line "Skipping latter actions!"

## Prevent the following task from executing with a successful outcome
## {} <-
##     when x is
##         Break ->
##             returnAfter
##                 (
##                     Stdout.line "Break!"
##                 )
##
##         Continue ->
##             \t ->
##                 {} <- Stdout.line "Continue!" |> await
##                 t {}
returnAfter = \last ->
    \_skip ->
        _ <- last |> await
        Task.ok {}
