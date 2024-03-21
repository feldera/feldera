interface Lib.List
    exposes [nubFirstOn]
    imports []

## Remove duplicate elements in a list. Keep only the last occurrence
nubFirstOn = \list, getKey ->
    list |> List.map (\e -> (getKey e, e)) |> Dict.fromList |> Dict.values

expect
    a = [(1, "a"), (1, "b"), (1, "c")] |> nubFirstOn (\(k, _) -> k)
    a == [(1, "c")]
