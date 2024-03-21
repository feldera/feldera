interface Lib.List
    exposes [nubOrdOn]
    imports []

# TODO: improve performance
nubOrdOn = \ project, list ->
  set = list |> List.map project |> Set.fromList
  list |> List.dropIf \ e -> Set.contains set (project e)