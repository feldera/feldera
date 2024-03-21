interface App.Cmd.Workspace
    exposes [workspaceCtx, putWorkspaceCtx, updateWorkspaceDefault, onConflictReplace, onConflictSkip, onConflictError ]
    imports [
      pf.Task.{ Task, await }, pf.Dir,  pf.Stdout, pf.Path, pf.File,
      json.Core.{jsonWithOptions},
      Lib.Some.{some, mapSome, fromSome, requireSome, fromSomeWith},
      Lib.List.{nubOrdOn}
    ]

jsonDecoder = jsonWithOptions { fieldNameMapping: CamelCase }

# TODO: implement Decoding ability for Dict

# x: Result {default: [None, Some {url: Str}], known: List (Str, { url : Str })} a ->
#   Result {default: [None, Some {url: Str}], known: List (Str, { url : Str })} a
# x = \a -> a

felderaJsonPath = \ absoluteDir -> Str.concat absoluteDir  "/feldera.json" |> Path.fromStr

# workspaceCtx : Str -> Task {default: [None, Some {url: Str}], known: List (Str, { url : Str })} *
# workspaceCtx : Str -> Task [Some {default: {url: Str}, known: List (Str, { url : Str })}, None]*
## Get the known Feldera deployments
workspaceCtx = \absoluteDir ->
  # emptyJson = { default: None, known: [] }
  task =
    bytes <- felderaJsonPath absoluteDir |> File.readUtf8 |> Task.map Str.toUtf8 |> await
    # decorateDefault : Result {default: {url: Str}, known: List (Str, { url : Str })} a -> Result {default: [None, Some {url: Str}], known: List (Str, { url : Str })} a
    # decorateDefault = \result -> Result.map result \ctx -> { default: Some ctx.default, known: ctx.known }
    decorateDefault : Result {default: {url: Str}, known: List (Str, { url : Str })} a -> Result {default: {url: Str}, known: List (Str, { url : Str })} a
    decorateDefault = \result -> result
    # idType : Result {default: {url: Str}, known: List (Str, { url : Str })} a -> Result {default: {url: Str}, known: List (Str, { url : Str })} a
    felderaJson = Decode.fromBytes bytes jsonDecoder
      |> decorateDefault
      |> Result.map \j -> Some j
      |> Result.withDefault None
    Task.ok felderaJson
  task |> Task.onErr (\_ -> Task.ok None)

handleErr = \err -> when err is
    FileWriteErr _path e -> crash (Inspect.toStr e)

onConflictReplace = \ _old, new -> Ok new
onConflictSkip = \ old, _new -> Ok old
onConflictError = \ _old, _new -> Err {}

## Set url as default, store it in known list, avoid duplicates, overwrite existing name
# updateWorkspaceDefault: a, b, c -> Task Str {}
updateWorkspaceDefault = \ {url, name}, absoluteDir, onConflict ->
  ctx <- workspaceCtx absoluteDir
    |> Task.map (\someCtx ->
        default = someCtx
            |> mapSome (.default)
            |> (\u -> some (Some {url}) u) |> requireSome "No URL provided"
        knownWithResolvedConflicts = someCtx
          |> fromSomeWith .known []
          |> List.mapTry (\e ->
            # Replace all known workspaces with matching name using conflict resolution strategy
            (n,_) = e
            if n == name
              then onConflict e (name, {url: url})
              else if n == url # If found unnamed workspace with a matching URL
                then Ok (name, {url: url}) # Name it with a given name
                else Ok e
          )
          |> \result -> when result is
            # Ok list -> if List.findFirst list (\(n, _) -> n == name ) |> Result.isOk
            #   then list
            #   else List.append list (name, {url})
            Ok list -> List.append list (name, {url})
            Err _ -> crash "updateWorkspaceDefault onConflict error"
          |> nubOrdOn \(n,_) -> n
        {
          default,
          known: knownWithResolvedConflicts
        }
    )
    |> await
  dbg "updateWorkspaceDefault putWorkspaceCtx $(Inspect.toStr ctx)"
  putWorkspaceCtx absoluteDir ctx

putWorkspaceCtx = \absoluteDir, ctx ->
  s = Encode.toBytes ctx jsonDecoder |> Str.fromUtf8 |> Result.withDefault "{ error: \"Utf error\" }"
  {} <- File.writeUtf8 (felderaJsonPath absoluteDir) s
    |> Task.onErr handleErr
    |> await
  Task.ok ctx