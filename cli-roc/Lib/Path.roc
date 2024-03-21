interface Lib.Path
    exposes [pathDirectory]
    imports [pf.Path]

## Returns the directory of the file, or directory itself
# https://github.com/roc-lang/basic-cli/blob/0.7.0/src/InternalPath.roc
pathDirectory = \ path ->
  str = Path.display path
  when str |> Str.splitLast "/" is
  Ok {before} -> before
  Err NotFound -> str