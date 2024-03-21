interface Lib.Nullable
    exposes [Nullable, null, full, requireFull, fromNull, nullable, isNull, nullResult]
    imports [
        json.Core.{ json },
    ]

Nullable val := [Null, Full val] implements [
        Decoding { decoder: nullableDecode },
        Encoding { toEncoder: nullableEncode },
        Inspect, # auto derive
        Eq, # auto derive
    ]

null = @Nullable Null

full = \a -> @Nullable (Full a)

requireFull : Nullable a -> a
requireFull = \@Nullable n ->
    when n is
        Full a -> a
        Null -> crash "requireFull called on null"

fromNull : Nullable a, a -> a
fromNull = \@Nullable n, default ->
    when n is
        Full a -> a
        Null -> default

nullable : Nullable a, b, (a -> b) -> b
nullable = \@Nullable n, default, onFull ->
    when n is
        Full a -> onFull a
        Null -> default

isNull = \n ->
    when n is
        @Nullable Null -> Bool.true
        _ -> Bool.false

nullResult = \n, err ->
    when n is
        @Nullable Null -> Err err
        @Nullable (Full a) -> Ok a

nullBytes = [110, 117, 108, 108]

nullableDecode : Decoder (Nullable val) fmt where val implements Decoding, fmt implements DecoderFormatting
nullableDecode = Decode.custom \bytes, fmt ->
    if bytes |> List.startsWith nullBytes then
        { result: Ok (@Nullable (Null)), rest: List.dropFirst bytes 4 }
    else
        when bytes |> Decode.decodeWith (Decode.decoder) fmt is
            { result: Ok res, rest } -> { result: Ok (@Nullable (Full res)), rest }
            { result: Err a, rest } -> { result: Err a, rest }

nullableEncode : Nullable val -> Encoder fmt where val implements Encoding, fmt implements EncoderFormatting
nullableEncode = \val ->
    Encode.custom
        (\bytes, fmt -> List.concat
                bytes
                (
                    when val is
                        @Nullable Null -> nullBytes
                        @Nullable (Full a) -> Encode.toBytes a fmt
                )
        )

TestType : {
    a : Nullable I32,
}

expect
    actual : DecodeResult TestType
    actual = "{\"a\":null}" |> Str.toUtf8 |> Decode.fromBytesPartial json
    expected = Ok ({ a: null })
    actual.result == expected

expect
    actual : DecodeResult TestType
    actual = "{\"a\":3}" |> Str.toUtf8 |> Decode.fromBytesPartial json
    expected = Ok ({ a: full 3 })
    actual.result == expected

expect
    actual = { a: null } |> Encode.toBytes json |> Str.fromUtf8
    expected = Ok ("{\"a\":null}")
    actual == expected

expect
    actual = { a: full 3 } |> Encode.toBytes json |> Str.fromUtf8
    expected = Ok ("{\"a\":3}")
    actual == expected
