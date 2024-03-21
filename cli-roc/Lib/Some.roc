interface Lib.Some
    exposes [some, mapSome, fromSome, requireSome, fromSomeWith]
    imports []

# isSome a = when a is
#     Some _ -> true
#     _ -> false

some = \a, b -> when [a, b] is
    [Some x, _] -> Some x
    [_, Some y] -> Some y
    _ -> None

# Typ a b: a -> b

# mapSome : [Some a]*, Typ a b -> [Some b, None]
mapSome = \a, f -> when a is
    Some v -> Some (f v)
    _ -> None

fromSome = \a, d -> when a is
    Some v -> v
    _ -> d

requireSome = \a, err -> when a is
    Some v -> v
    _ -> crash err

fromSomeWith = \a, f, d -> when a is
    Some v -> f v
    _ -> d