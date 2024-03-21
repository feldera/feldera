interface Lib.Show
    exposes [Show, show]
    imports [
    ]

Show implements
    show : a -> Str where a implements Show
