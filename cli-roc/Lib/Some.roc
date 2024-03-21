interface Lib.Some
    exposes [some, someOf, mapSome, fromSome, requireSome]
    imports []

# isSome a = when a is
#     Some _ -> true
#     _ -> false

someOf = \a, b ->
    when (a, b) is
        (Some x, _) -> Some x
        (_, Some y) -> Some y
        _ -> None

requireSome = \a, err ->
    when a is
        Some v -> v
        _ -> crash err

mapSome = \a, f ->
    when a is
        Some v -> Some (f v)
        _ -> None

some = \a, d, f ->
    when a is
        Some v -> f v
        _ -> d

fromSome = \a, d ->
    when a is
        Some v -> v
        _ -> d
