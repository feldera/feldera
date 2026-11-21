"""Preselected runtime edge vectors and SQL constant-required/warning cases."""

import itertools
import json
from decimal import Decimal


def spatial_cases():
    def point(x, y):
        return {
            "kind": "ST_POINT",
            "operator": "ST_POINT",
            "builtin": "calcite-point-2d-v1",
            "deterministic": True,
            "type": {"name": "GEOMETRY", "precision": -1, "scale": 0},
            "arguments": [
                {
                    "kind": "LITERAL",
                    "type": {"name": "DOUBLE", "precision": 15, "scale": 0},
                    "value": n,
                }
                for n in (x, y)
            ],
        }

    for index, coordinates in enumerate(
        itertools.product([None, "0", "-1", "1e-200", "1e200"], repeat=4)
    ):
        yield {
            "id": f"spatial/distance/{index}",
            "operation": "spatial/distance",
            "value": json.dumps(coordinates),
            "expression": {
                "kind": "OTHER_FUNCTION",
                "operator": "ST_DISTANCE",
                "builtin": "calcite-point-distance-v1",
                "deterministic": True,
                "type": {"name": "DOUBLE", "precision": 15, "scale": 0},
                "arguments": [point(*coordinates[:2]), point(*coordinates[2:])],
            },
        }


def runtime_cases():
    yield from spatial_cases()
    groups = {
        "uuid": [
            "123e4567-e89b-12d3-a456-426655440000",
            "{123E4567-E89B-12D3-A456-426655440000}",
            "123e-4567-e89b-12d3-a456-4266-5544-0000",
            "123e4567e89b12d3a456426655440000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "00000000000000000000000000000000",
            " 123e4567-e89b-12d3-a456-426655440000",
            "123e4567-e89b-12d3-a456-426655440000 ",
            "urn:uuid:123e4567-e89b-12d3-a456-426655440000",
            "123e--4567e89b12d3a456426655440000",
            "123-e4567e89b12d3a456426655440000",
            "123e4567e89b12d3a456426655440000-",
            "{123e4567e89b12d3a456426655440000",
            "123e4567e89b12d3a4564266554400000",
            "g23e4567e89b12d3a456426655440000",
            "",
        ],
        "date": [
            "2024-02-29",
            "\t2024-02-29\n",
            "0000-01-01",
            "+10000-01-01",
            "2024-02-30",
            "\u00a02024-01-01",
        ],
        "time": [
            "12:34:56",
            " 12:34:56.123456789 ",
            "23:59:60",
            "00:00:00",
            "24:00:00",
            "bad",
        ],
        "timestamp": [
            "2024-02-29",
            " 2024-02-29 23:59:59.123456789 ",
            "2024-02-29 23:59:60",
            "0000-01-01",
            "+10000-01-01",
            "1969-12-31 23:59:59.999999999",
            "bad",
        ],
        "decimal_4_2": [
            "12.345",
            "-12.345",
            "99.999",
            "100.00",
            "-100.00",
            "0",
            "0.005",
            "-0.005",
        ],
        "initcap": [
            "",
            "aBC dEF",
            "ggj12341jhg12341=-012341asdf",
            "1abc-DEF",
            "  one\ttWO\nTHREE",
        ],
        "upper": ["straße", "İıiI", "🦀 abc"],
        "lower": ["ΟΣ", "İıiI", "🦀 ABC"],
        "length": ["e\u0301", "🦀", "abc", ""],
        "float_cast": ["-0", "0", "NaN", "-NaN", "Infinity", "-Infinity"],
        "float_cast_real": ["-0", "0", "NaN", "-NaN", "Infinity", "-Infinity"],
    }
    for operation, values in groups.items():
        for index, value in enumerate(values):
            literal = {
                "kind": "LITERAL",
                "type": {
                    "name": "DECIMAL" if operation == "decimal_4_2" else "VARCHAR",
                    "precision": 38 if operation == "decimal_4_2" else -1,
                    "scale": 18 if operation == "decimal_4_2" else 0,
                },
                "value": value,
            }
            function = {
                "initcap": "INITCAP",
                "upper": "UPPER",
                "lower": "LOWER",
                "length": "CHAR_LENGTH",
            }.get(operation)
            expression = {
                "kind": "OTHER_FUNCTION" if function else "CAST",
                "operator": function or "CAST",
                "deterministic": True,
                "type": {
                    "name": {
                        "decimal_4_2": "DECIMAL",
                        "initcap": "VARCHAR",
                        "upper": "VARCHAR",
                        "lower": "VARCHAR",
                        "length": "INTEGER",
                        "float_cast": "DOUBLE",
                        "float_cast_real": "REAL",
                    }.get(operation, operation.upper()),
                    "precision": 4 if operation == "decimal_4_2" else -1,
                    "scale": 2 if operation == "decimal_4_2" else 0,
                },
                "arguments": [literal],
            }
            yield {
                "id": f"{operation}/{index}",
                "operation": operation,
                "value": value,
                "expression": expression,
            }

    def literal(name, value):
        return {
            "kind": "LITERAL",
            "type": {"name": name, "precision": -1, "scale": 0},
            "value": value,
        }

    for operation, left_type, right_type, (index, pair) in itertools.product(
        ("plus", "minus", "times", "divide", "mod"),
        ("DECIMAL", "INTEGER"),
        ("DECIMAL", "INTEGER"),
        enumerate(
            (
                ("5.25", "2.125"),
                ("-5.25", "2.125"),
                ("5.25", "-2.125"),
                ("-5.25", "-2.125"),
                ("0.809", "100"),
                ("0", "0"),
                (None, "0"),
                ("1", None),
            )
        ),
    ):
        values = [
            str(int(Decimal(value)))
            if value is not None and kind == "INTEGER"
            else value
            for kind, value in zip((left_type, right_type), pair)
        ]
        arguments = [
            {
                "kind": "LITERAL",
                "type": {"name": kind, "precision": 12, "scale": 3},
                "value": value,
            }
            for kind, value in zip((left_type, right_type), values)
        ]
        yield {
            "id": f"decimal_arithmetic/{operation}/{left_type}/{right_type}/{index}",
            "operation": f"decimal_arithmetic/{operation}",
            "value": json.dumps(values),
            "expression": {
                "kind": operation.upper(),
                "deterministic": True,
                "type": {"name": "DECIMAL", "precision": 20, "scale": 3},
                "arguments": arguments,
            },
        }

    selection_values = {
        "BIGINT": ("-9223372036854775808", "0", "9223372036854775807"),
        "DECIMAL": ("-1.125", "0.000", "1.125"),
        "DOUBLE": ("-Infinity", "-0.0", "NaN", "Infinity"),
        "VARCHAR": ("", "é", "\ue000", "🦀"),
    }
    for kind, domain in selection_values.items():
        pairs = itertools.product((None, *domain), repeat=2)
        for index, pair in enumerate(pairs):
            for operation in (
                "nullif",
                "least",
                "greatest",
                "least_pg",
                "greatest_pg",
                "sort_asc",
                "sort_desc",
            ):
                arguments = [
                    {
                        "kind": "LITERAL",
                        "type": {"name": kind, "precision": 12, "scale": 3},
                        "value": v,
                    }
                    for v in pair
                ]
                if operation.startswith("sort_"):
                    arguments = [
                        {
                            "kind": "ARRAY_VALUE_CONSTRUCTOR",
                            "operator": "ARRAY",
                            "deterministic": True,
                            "type": {"name": "ARRAY", "precision": -1, "scale": 0},
                            "arguments": arguments,
                        },
                        {
                            "kind": "LITERAL",
                            "type": {"name": "BOOLEAN", "precision": -1, "scale": 0},
                            "value": "true" if operation == "sort_asc" else "false",
                        },
                    ]
                yield {
                    "id": f"selection/{kind}/{operation}/{index}",
                    "operation": f"selection/{kind}/{operation}",
                    "value": json.dumps(pair),
                    "expression": {
                        "kind": "SORT_ARRAY"
                        if operation.startswith("sort_")
                        else operation.upper(),
                        "operator": "SORT_ARRAY"
                        if operation.startswith("sort_")
                        else operation.upper(),
                        "deterministic": True,
                        "type": {
                            "name": "ARRAY" if operation.startswith("sort_") else kind,
                            "precision": 12,
                            "scale": 3,
                        },
                        "arguments": arguments,
                    },
                }

    for kind, operation, unit, index, value in temporal_rounding_cases():
        yield {
            "id": f"round/{kind}/{operation}/{unit}/{index}",
            "operation": f"round/{kind}/{operation}/{unit.lower()}",
            "value": value,
            "expression": {
                "kind": "DATE_TRUNC" if operation == "trunc" else operation.upper(),
                "deterministic": True,
                "type": {"name": kind.upper(), "precision": -1, "scale": 0},
                "arguments": [literal(kind.upper(), value), literal("SYMBOL", unit)],
            },
        }

    for runtime_type, sql_type, maximum in (
        ("u8", "UTINYINT", 2**8 - 1),
        ("u16", "USMALLINT", 2**16 - 1),
        ("u32", "UINTEGER", 2**32 - 1),
        ("u64", "UBIGINT", 2**64 - 1),
    ):
        for source, source_type, inputs in (
            (
                "s",
                "VARCHAR",
                [
                    "0",
                    "+0",
                    "-0",
                    "-1",
                    "1",
                    " \t12\n",
                    "\u00a012",
                    "1.0",
                    "",
                    str(maximum),
                    str(maximum + 1),
                    "18446744073709551616",
                ],
            ),
            (
                "i64",
                "BIGINT",
                [
                    str(-(2**63)),
                    "-1",
                    "0",
                    "1",
                    "255",
                    "256",
                    "65535",
                    "65536",
                    "4294967295",
                    "4294967296",
                    str(2**63 - 1),
                ],
            ),
            ("b", "BOOLEAN", ["false", "true"]),
        ):
            for index, value in enumerate(inputs):
                yield {
                    "id": f"unsigned/{runtime_type}/{source}/{index}",
                    "operation": f"unsigned_{runtime_type}_{source}",
                    "value": value,
                    "expression": {
                        "kind": "CAST",
                        "operator": "CAST",
                        "deterministic": True,
                        "type": {"name": sql_type, "precision": -1, "scale": 0},
                        "arguments": [literal(source_type, value)],
                    },
                }

    for operation in (
        "left2",
        "right2",
        "substring_0_3",
        "repeat2",
        "position_e",
        "replace_e",
    ):
        for index, value in enumerate(("", "éclair", "e\u0301é")):
            text = literal("VARCHAR", value)
            function, arguments = {
                "left2": ("LEFT", [text, literal("INTEGER", "2")]),
                "right2": ("RIGHT", [text, literal("INTEGER", "2")]),
                "substring_0_3": (
                    "SUBSTRING",
                    [text, literal("INTEGER", "0"), literal("INTEGER", "3")],
                ),
                "repeat2": ("REPEAT", [text, literal("INTEGER", "2")]),
                "position_e": ("POSITION", [literal("VARCHAR", "é"), text]),
                "replace_e": (
                    "REPLACE",
                    [text, literal("VARCHAR", "é"), literal("VARCHAR", "E")],
                ),
            }[operation]
            yield {
                "id": f"{operation}/{index}",
                "operation": operation,
                "value": value,
                "expression": {
                    "kind": "OTHER_FUNCTION",
                    "operator": function,
                    "deterministic": True,
                    "type": {
                        "name": "INTEGER" if operation == "position_e" else "VARCHAR",
                        "precision": -1,
                        "scale": 0,
                    },
                    "arguments": arguments,
                },
            }
    for operation, values, function, suffix in [
        ("md5", ("", "Feldera", "é😀"), "MD5", []),
        ("like_suffix", ("", "a", "éclair"), "LIKE", [literal("VARCHAR", "%_")]),
        (
            "like_escape",
            ("a%b", "axb", "a%"),
            "LIKE",
            [literal("VARCHAR", "a!%_"), literal("VARCHAR", "!")],
        ),
        ("ilike_h", ("Hawkeye", "hawk", "other"), "ILIKE", [literal("VARCHAR", "h%")]),
    ]:
        for index, value in enumerate(values):
            yield {
                "id": f"{operation}/{index}",
                "operation": operation,
                "value": value,
                "expression": {
                    "kind": "OTHER_FUNCTION" if function == "MD5" else "LIKE",
                    "operator": function,
                    "deterministic": True,
                    "type": {
                        "name": "VARCHAR" if function == "MD5" else "BOOLEAN",
                        "precision": -1,
                        "scale": 0,
                    },
                    "arguments": [literal("VARCHAR", value), *suffix],
                },
            }
    for operation in (
        "round_decimal1",
        "truncate_decimal1",
        "round_float1",
        "truncate_float1",
    ):
        decimal = "decimal" in operation
        values = ("1.25", "-1.25", "1.29", "-1.29", "0", "99.99")
        if not decimal:
            values += ("NaN", "Infinity", "-Infinity", "-0", "1e308", "1e-308")
        for index, value in enumerate(values):
            result_type = {
                "name": "DECIMAL" if decimal else "DOUBLE",
                "precision": 12 if decimal else -1,
                "scale": 4 if decimal else 0,
            }
            yield {
                "id": f"{operation}/{index}",
                "operation": operation,
                "value": value,
                "expression": {
                    "kind": "OTHER_FUNCTION",
                    "operator": "ROUND"
                    if operation.startswith("round")
                    else "TRUNCATE",
                    "deterministic": True,
                    "type": result_type,
                    "arguments": [
                        {"kind": "LITERAL", "type": result_type, "value": value},
                        literal("INTEGER", "1"),
                    ],
                },
            }

    def request(operation, index, value, kind, function, arguments, result_type):
        return {
            "id": f"{operation}/{index}",
            "operation": operation,
            "value": value,
            "expression": {
                "kind": kind,
                "operator": function,
                "deterministic": True,
                "type": {"name": result_type, "precision": -1, "scale": 0},
                "arguments": arguments,
            },
        }

    for key_type, keys in (
        ("BIGINT", [-1, 0, 1, 2]),
        ("VARCHAR", ["", "a", "🦀", "missing"]),
        ("ARRAY", [[], [1], [1, None], [2]]),
    ):

        def key_expression(key, key_type=key_type):
            if key is None:
                return literal(key_type, None)
            if key_type == "ARRAY":
                return {
                    "kind": "OTHER_FUNCTION",
                    "operator": "ARRAY",
                    "deterministic": True,
                    "type": {"name": "ARRAY", "precision": -1, "scale": 0},
                    "arguments": [
                        literal("BIGINT", None if v is None else str(v)) for v in key
                    ],
                }
            return literal(key_type, str(key))

        def map_expression(entries):
            if entries is None:
                return literal("MAP", None)
            return {
                "kind": "MAP_VALUE_CONSTRUCTOR",
                "deterministic": True,
                "type": {"name": "MAP", "precision": -1, "scale": 0},
                "arguments": [
                    arg
                    for key, value in entries
                    for arg in (key_expression(key), literal("VARCHAR", value))
                ],
            }

        maps = [
            None,
            [],
            [(keys[0], "a"), (keys[1], None)],
            [(keys[0], "old"), (keys[0], "new"), (keys[2], "🦀")],
        ]
        for operation in ("construct", "cardinality", "contains", "item", "concat"):
            pairs = (
                itertools.product(maps, maps)
                if operation == "concat"
                else ((m, None) for m in maps)
            )
            for index, (left, right) in enumerate(pairs):
                for key_index, key in enumerate([None, *keys]):
                    operands = [map_expression(left)]
                    if operation == "concat":
                        operands.append(map_expression(right))
                    elif operation in ("contains", "item"):
                        operands.append(key_expression(key))
                    function = {
                        "construct": "MAP_VALUE_CONSTRUCTOR",
                        "cardinality": "CARDINALITY",
                        "contains": "MAP_CONTAINS_KEY",
                        "item": "ITEM",
                        "concat": "MAP_CONCAT",
                    }[operation]
                    case = request(
                        f"map/{key_type}/{operation}",
                        f"{index}/{key_index}",
                        json.dumps(
                            {
                                "maps": [left, right]
                                if operation == "concat"
                                else [left],
                                "key": key,
                            }
                        ),
                        function
                        if operation in ("item", "contains", "concat")
                        else "OTHER_FUNCTION",
                        function,
                        operands,
                        {
                            "cardinality": "INTEGER",
                            "contains": "BOOLEAN",
                            "item": "VARCHAR",
                        }.get(operation, "MAP"),
                    )
                    if operation == "construct":
                        case["expression"] = map_expression(left)
                    yield case

    for array, separator, replacement, arity in itertools.product(
        (None, [], [None], ["", None, "🦀", "é"], ["one", "two"]),
        (None, "", ",", "🦀"),
        (None, "", "NULL"),
        (2, 3),
    ):
        arguments = [
            literal("ARRAY", None)
            if array is None
            else {
                "kind": "ARRAY_VALUE_CONSTRUCTOR",
                "type": {"name": "ARRAY", "precision": -1, "scale": 0},
                "deterministic": True,
                "arguments": [literal("VARCHAR", v) for v in array],
            },
            literal("VARCHAR", separator),
        ]
        if arity == 3:
            arguments.append(literal("VARCHAR", replacement))
        payload = {"values": array, "separator": separator, "replacement": replacement}
        yield request(
            f"text/join{arity}",
            json.dumps(payload, ensure_ascii=False),
            json.dumps(payload),
            "OTHER_FUNCTION",
            "ARRAY_TO_STRING",
            arguments,
            "VARCHAR",
        )

    for index, values in enumerate(itertools.product((None, "", "a", "🦀"), repeat=3)):
        yield request(
            "text/concat",
            index,
            json.dumps(values),
            "OTHER_FUNCTION",
            "CONCAT",
            [literal("VARCHAR", v) for v in values],
            "VARCHAR",
        )

    for index, (value, size, fixed) in enumerate(
        itertools.product(
            (
                None,
                "0001-01-01",
                "0999-12-31",
                "2024-02-29",
                "0000-01-01",
                "-0001-01-01",
                "+10000-01-01",
            ),
            (-1, 0, 4, 10, 12),
            (False, True),
        )
    ):
        # A fixed CHAR requires a nonnegative width in the runtime API.
        if fixed and size < 0:
            continue
        case = request(
            "text/date",
            index,
            json.dumps([value, size, fixed]),
            "CAST",
            "CAST",
            [literal("DATE", value)],
            "CHAR" if fixed else "VARCHAR",
        )
        case["expression"]["type"]["precision"] = size
        yield case

    for kind, bits in (
        ("TINYINT", 8),
        ("SMALLINT", 16),
        ("INTEGER", 32),
        ("BIGINT", 64),
        ("UTINYINT", 8),
        ("USMALLINT", 16),
        ("UINTEGER", 32),
        ("UBIGINT", 64),
    ):
        unsigned = kind.startswith("U")
        values = [None, 0, 1, 10, 2 ** (bits - int(not unsigned)) - 1]
        values += [128] if unsigned else [-1, -(2 ** (bits - 1))]
        for target, precision in itertools.product(
            ("BINARY", "VARBINARY"), (-1, 0, 1, 2, 4, 8, 10)
        ):
            for index, value in enumerate(values):
                operation = f"integer_binary/{kind}/{target}/{precision}"
                case = request(
                    operation,
                    index,
                    "null" if value is None else str(value),
                    "CAST",
                    "CAST",
                    [literal(kind, None if value is None else str(value))],
                    target,
                )
                case["expression"]["type"]["precision"] = precision
                yield case

    for function, operation, result_type in (
        ("MD5", "binary_md5", "VARCHAR"),
        ("STARTS_WITH", "binary_starts_ff", "BOOLEAN"),
        ("ENDS_WITH", "binary_ends_ff", "BOOLEAN"),
        ("POSITION", "binary_position_ff", "INTEGER"),
        ("POSITION", "binary_position_empty", "INTEGER"),
    ):
        for index, value in enumerate(
            ("", "ff", "00", "ff00", "00ff", "ff00ff", "c3a9", "ffff")
        ):
            arguments = [literal("VARBINARY", value)]
            if function == "POSITION":
                arguments.insert(
                    0, literal("VARBINARY", "" if operation.endswith("empty") else "ff")
                )
            elif function != "MD5":
                arguments.append(literal("VARBINARY", "ff"))
            yield request(
                operation,
                index,
                value,
                "OTHER_FUNCTION" if function == "MD5" else function,
                function,
                arguments,
                result_type,
            )
    for index, value in enumerate(
        (
            "",
            "00",
            "00" * 15,
            "00" * 16,
            "ff" * 16,
            "00" * 17,
            "123e4567e89b12d3a456426655440000",
        )
    ):
        yield request(
            "binary_to_uuid",
            index,
            value,
            "CAST",
            "CAST",
            [literal("VARBINARY", value)],
            "UUID",
        )
    for operation, name, precision in (
        ("all", "VARBINARY", -1),
        ("fixed20", "BINARY", 20),
        ("var4", "VARBINARY", 4),
    ):
        for index, value in enumerate(
            (
                "00000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff",
                "123e4567-e89b-12d3-a456-426655440000",
            )
        ):
            row = request(
                "binary_uuid_" + operation,
                index,
                value,
                "CAST",
                "CAST",
                [literal("UUID", value)],
                name,
            )
            row["expression"]["type"]["precision"] = precision
            yield row

    for operation in (
        "binary_fixed4",
        "binary_var2",
        "binary_left2",
        "binary_right2",
        "binary_substring0_3",
        "binary_concat",
        "binary_hex",
    ):
        for index, value in enumerate(("", "00ff80", "c3a9", "ff00017f")):
            kind, function, result_type = "OTHER_FUNCTION", "", "VARBINARY"
            args = [literal("VARBINARY", value)]
            if operation in ("binary_fixed4", "binary_var2"):
                kind = function = "CAST"
                result_type = "BINARY" if operation == "binary_fixed4" else "VARBINARY"
            elif operation in ("binary_left2", "binary_right2"):
                function = "LEFT" if operation == "binary_left2" else "RIGHT"
                args.append(literal("INTEGER", "2"))
            elif operation == "binary_substring0_3":
                function = "SUBSTRING"
                args += [literal("INTEGER", "0"), literal("INTEGER", "3")]
            elif operation == "binary_concat":
                kind, function = "OTHER", "||"
                args.append(literal("BINARY", "00ff"))
            else:
                function, result_type = "TO_HEX", "VARCHAR"
            row = request(operation, index, value, kind, function, args, result_type)
            if operation in ("binary_fixed4", "binary_var2"):
                row["expression"]["type"]["precision"] = (
                    4 if operation == "binary_fixed4" else 2
                )
            yield row
    for index, value in enumerate(("", "é", "🦀", "a\x00b")):
        row = request(
            "binary_utf8",
            index,
            value,
            "CAST",
            "CAST",
            [literal("VARCHAR", value)],
            "VARBINARY",
        )
        yield row
    for operation in ("binary_overlay3", "binary_overlay4"):
        for index, value in enumerate(("00ff80", "1234567890")):
            args = [
                literal("VARBINARY", value),
                literal("BINARY", "aabb"),
                literal("INTEGER", "2"),
            ]
            if operation == "binary_overlay4":
                args.append(literal("INTEGER", "0"))
            yield request(
                operation, index, value, "OTHER_FUNCTION", "OVERLAY", args, "VARBINARY"
            )

    for target in (
        "YEAR",
        "MONTH",
        "YEAR_MONTH",
        "DAY",
        "HOUR",
        "DAY_HOUR",
        "MINUTE",
        "DAY_MINUTE",
        "HOUR_MINUTE",
        "SECOND",
        "DAY_SECOND",
        "HOUR_SECOND",
        "MINUTE_SECOND",
    ):
        months = target in ("YEAR", "MONTH", "YEAR_MONTH")
        values = (
            (0, 1, -1, 19, -19, -(2**31), 2**31 - 1)
            if months
            else (0, 1, -1, 90000001, -90000001, -(2**63), 2**63 - 1)
        )
        for index, value in enumerate(values):
            yield request(
                f"interval_cast_{target}",
                index,
                str(value),
                "CAST",
                "CAST",
                [
                    literal(
                        "INTERVAL_MONTH" if months else "INTERVAL_SECOND",
                        str(value) if months else str(Decimal(value) / 1000),
                    )
                ],
                f"INTERVAL_{target}",
            )

    for name in (
        "sin",
        "cos",
        "tan",
        "cot",
        "coth",
        "csch",
        "sec",
        "csc",
        "asin",
        "acos",
        "atan",
        "ln",
        "log",
        "log10",
        "exp",
        "degrees",
        "radians",
        "log4",
        "power2",
        "atan2",
        "neg",
        "mulzero",
        "divzero",
        "floor",
        "ceil",
    ):
        for index, value in enumerate(
            ("-1", "-0", "0", "0.5", "1", "2", "3", "NaN", "Infinity")
        ):
            arguments = [literal("DOUBLE", value)]
            if name in ("log4", "power2", "atan2"):
                arguments.append(literal("DOUBLE", "4" if name == "log4" else "2"))
            if name in ("mulzero", "divzero"):
                arguments.append(literal("DOUBLE", "0"))
            function = {"log4": "LOG", "power2": "POWER"}.get(name, name.upper())
            kind = {
                "neg": "MINUS_PREFIX",
                "mulzero": "TIMES",
                "divzero": "DIVIDE",
                "floor": "FLOOR",
                "ceil": "CEIL",
            }.get(name, "OTHER_FUNCTION")
            yield request(
                "math_" + name,
                index,
                value,
                kind,
                function,
                arguments,
                "DOUBLE",
            )
    for operation in ("overlay3", "overlay4"):
        for index, value in enumerate(("abcd", "éclair", "e\u0301é")):
            arguments = [
                literal("VARCHAR", value),
                literal("VARCHAR", "é" if operation == "overlay3" else "ab"),
                literal("INTEGER", "2"),
            ]
            if operation == "overlay4":
                arguments.append(literal("INTEGER", "0"))
            yield request(
                operation,
                index,
                value,
                "OTHER_FUNCTION",
                "OVERLAY",
                arguments,
                "VARCHAR",
            )
    for operation in ("split_pipe", "trim_x", "concat_ws_null"):
        for index, value in enumerate(("", "|é|", "xxéxx")):
            text = literal("VARCHAR", value)
            kind, function, arguments, result_type = {
                "split_pipe": (
                    "OTHER_FUNCTION",
                    "SPLIT",
                    [text, literal("VARCHAR", "|")],
                    "ARRAY",
                ),
                "trim_x": (
                    "TRIM",
                    "TRIM",
                    [literal("SYMBOL", "BOTH"), literal("VARCHAR", "x"), text],
                    "VARCHAR",
                ),
                "concat_ws_null": (
                    "OTHER_FUNCTION",
                    "CONCAT_WS",
                    [literal("VARCHAR", "|"), literal("VARCHAR", None), text],
                    "VARCHAR",
                ),
            }[operation]
            yield request(
                operation, index, value, kind, function, arguments, result_type
            )
    inner = request(
        "",
        0,
        "",
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        [literal("DOUBLE", "NaN"), literal("DOUBLE", None)],
        "ARRAY",
    )["expression"]
    nested = request("", 0, "", "ARRAY_VALUE_CONSTRUCTOR", "ARRAY", [inner], "ARRAY")[
        "expression"
    ]
    yield request("array_nan_equal", 0, "", "EQUALS", "=", [nested, nested], "BOOLEAN")
    for operation in ("array_distinct", "array_min", "array_max"):
        for index, values in enumerate(([], [None], [None, 2, 1, None, 2], [-1, 0, 1])):
            array = request(
                "",
                0,
                "",
                "ARRAY_VALUE_CONSTRUCTOR",
                "ARRAY",
                [literal("BIGINT", str(v) if v is not None else None) for v in values],
                "ARRAY",
            )["expression"]
            yield request(
                operation,
                index,
                json.dumps(values),
                operation.upper(),
                operation.upper(),
                [array],
                "ARRAY" if operation == "array_distinct" else "BIGINT",
            )
    for unit in ("CENTURY", "ISOYEAR", "WEEK", "DOW"):
        for index, value in enumerate(
            ("1900-12-31", "1901-01-01", "2021-01-01", "2024-03-03")
        ):
            yield request(
                "extract_date_" + unit.lower(),
                index,
                value,
                "EXTRACT",
                "EXTRACT",
                [literal("SYMBOL", unit), literal("DATE", value)],
                "BIGINT",
            )
    for unit in ("EPOCH", "MICROSECOND"):
        for index, value in enumerate(
            ("1969-12-31 23:59:59.500000", "2024-02-29 01:02:03.123456")
        ):
            yield request(
                "extract_timestamp_" + unit.lower(),
                index,
                value,
                "EXTRACT",
                "EXTRACT",
                [literal("SYMBOL", unit), literal("TIMESTAMP", value)],
                "BIGINT",
            )
    for index, value in enumerate(("NaN", "-0", "0", "-1", "Infinity", "-Infinity")):
        yield {
            "id": f"float_lt_zero/{index}",
            "operation": "float_lt_zero",
            "value": value,
            "expression": {
                "kind": "LESS_THAN",
                "operator": "<",
                "deterministic": True,
                "type": {"name": "BOOLEAN", "precision": -1, "scale": 0},
                "arguments": [literal("DOUBLE", value), literal("DOUBLE", "0")],
            },
        }


def temporal_rounding_cases():
    dates = [
        "0001-01-01",
        "0005-06-01",
        "0099-12-31",
        "0100-01-01",
        "0999-12-31",
        "1000-01-01",
        "1900-03-01",
        "1969-12-31",
        "1970-01-01",
        "1999-12-31",
        "2000-01-01",
        "2000-02-29",
        "2001-01-01",
        "2024-03-31",
        "2024-04-01",
        "9999-12-31",
    ]
    calendar_units = [
        "MILLENNIUM",
        "CENTURY",
        "DECADE",
        "YEAR",
        "QUARTER",
        "MONTH",
        "WEEK",
        "DAY",
    ]
    clock_units = [
        "HOUR",
        "MINUTE",
        "SECOND",
        "MILLISECOND",
        "MICROSECOND",
        "NANOSECOND",
    ]
    for operation in ("floor", "ceil", "trunc"):
        units = calendar_units + (clock_units if operation != "trunc" else [])
        for unit in units:
            for index, date in enumerate(dates):
                yield "date", operation, unit, index, date
                for clock_index, clock in enumerate(("00:00:00", "23:59:59.999999")):
                    yield (
                        "timestamp",
                        operation,
                        unit,
                        index * 2 + clock_index,
                        f"{date} {clock}",
                    )


def sql_cases():
    """Positive and negative requests; no compiler outcome filters selection."""
    table = "CREATE TABLE t(x INT NOT NULL, ts TIMESTAMP NOT NULL);\n"
    programs = []
    interval_casts = [
        "CAST(INTERVAL '19' MONTH AS INTERVAL YEAR)",
        "CAST(INTERVAL '-19' MONTH AS INTERVAL YEAR)",
        "CAST(INTERVAL '1' YEAR AS INTERVAL MONTH)",
        "CAST(INTERVAL '90.500001' SECOND AS INTERVAL MINUTE)",
        "CAST(INTERVAL '-90.500001' SECOND AS INTERVAL MINUTE)",
        "CAST(INTERVAL '1.500001' SECOND AS INTERVAL DAY TO SECOND)",
        "CAST(INTERVAL '-25' HOUR AS INTERVAL DAY)",
    ]

    def add(site, expressions, template, *, needs_input=False):
        for index, expression in enumerate(expressions):
            prefix = table if needs_input else ""
            programs.append((f"{site}/{index}", prefix + template.format(e=expression)))

    add(
        "remaining_compatibility",
        [
            "CAST('nan' AS DOUBLE)",
            "CAST('  inf  ' AS REAL)",
            "LOG(2, 0)",
            "SPLIT('', '')",
            "MAP_CONCAT(MAP[1, 2], MAP[1.0, NULL])",
            "ST_DISTANCE(ST_POINT(0, 0), ST_POINT(0, 1))",
            "ST_DISTANCE(NULL, ST_POINT(0, 0))",
            "TIMESTAMP '2020-03-01 00:00:00' - INTERVAL '1.5' SECOND",
            "CAST('2059-10-5 5:3:37' AS TIMESTAMP)",
        ],
        "CREATE VIEW v AS SELECT {e} AS value;",
    )

    add(
        "array_initcap_hyperbolic",
        [
            "MAP_CONTAINS_KEY(MAP[ARRAY(1), ARRAY(2)], ARRAY(1))",
            "ARRAY_TO_STRING(ARRAY()::VARCHAR ARRAY, ',')",
            "INITCAP('aBC-1dEF ghi')",
            "INITCAP('ggj12341jhg12341=-012341asdf')",
            "COTH(0)",
            "CSCH(0)",
            "COTH(CAST(NULL AS DOUBLE))",
        ],
        "CREATE VIEW v AS SELECT {e} AS value;",
    )

    add(
        "map_folds",
        [
            "MAP[1, 'old', 1, 'new']",
            "CARDINALITY(MAP[1, 'a', 1, 'b'])",
            "MAP[1, 'a'][1]",
            "MAP[1, 'a'][2]",
            "MAP[1, CAST(NULL AS VARCHAR)][1]",
            "MAP_CONTAINS_KEY(MAP[1, CAST(NULL AS VARCHAR)], 1)",
            "MAP_CONTAINS_KEY(MAP[1, 'a'], CAST(NULL AS INT))",
            "MAP_CONTAINS_KEY(CAST(NULL AS MAP<INT, VARCHAR>), 1)",
            "MAP_CONTAINS_KEY(MAP[ARRAY[1, NULL], 'a'], ARRAY[1, NULL])",
            "MAP_CONCAT(MAP[1, 'a'], MAP[1, 'b'], MAP[2, 'c'])",
            "MAP_CONCAT(CAST(NULL AS MAP<INT, VARCHAR>), MAP[1, 'a'])",
        ],
        "CREATE VIEW v AS SELECT {e} AS value;",
    )

    add(
        "text_conversions",
        [
            "ARRAY_TO_STRING(ARRAY['a', NULL, '🦀'], ',')",
            "ARRAY_TO_STRING(ARRAY['a', NULL, '🦀'], ',', '*')",
            "ARRAY_TO_STRING(ARRAY['a', NULL], ',', CAST(NULL AS VARCHAR))",
            "ARRAY_TO_STRING(CAST(NULL AS VARCHAR ARRAY), ',')",
            "ARRAY_TO_STRING(ARRAY['a'], CAST(NULL AS VARCHAR))",
            "ARRAY_TO_STRING(CAST(ARRAY[] AS VARCHAR ARRAY), ',')",
            "CONCAT('a', '🦀', 'é')",
            "CONCAT('a', 1, TRUE)",
            "CONCAT('a', CAST(NULL AS VARCHAR), 'b')",
            "CAST(DATE '0001-01-01' AS VARCHAR)",
            "CAST(DATE '0999-12-31' AS CHAR(12))",
            "CAST(DATE '2024-02-29' AS VARCHAR(4))",
        ],
        "CREATE VIEW v AS SELECT {e} AS value;",
    )

    add(
        "integer_binary",
        [
            f"CAST(CAST({value} AS {source}) AS {target})"
            for source, value in (
                ("TINYINT", "-1"),
                ("SMALLINT", "256"),
                ("INTEGER", "16909060"),
                ("BIGINT", "-1"),
                ("UTINYINT", "255"),
                ("USMALLINT", "65535"),
                ("UINTEGER", "4294967295"),
                ("UBIGINT", "18446744073709551615"),
                ("INTEGER", "NULL"),
            )
            for target in ("BINARY(2)", "BINARY(10)", "VARBINARY(2)", "VARBINARY(10)")
        ],
        "CREATE VIEW v AS SELECT {e} AS value;",
    )

    add(
        "binary_search_uuid",
        [
            "STARTS_WITH(x'ff00', x'ff')",
            "ENDS_WITH(x'00ff', x'ff')",
            "POSITION(x'ff' IN x'00ffff')",
            "POSITION(x'' IN x'')",
            "MD5(x'ff00')",
            "CAST(x'123e4567e89b12d3a456426655440000' AS UUID)",
            "CAST(x'00' AS UUID)",
            "SAFE_CAST(x'00' AS UUID)",
            "CAST(UUID '123e4567-e89b-12d3-a456-426655440000' AS BINARY(20))",
            "CAST(UUID '123e4567-e89b-12d3-a456-426655440000' AS VARBINARY(4))",
        ],
        "CREATE VIEW v AS SELECT {e} AS value;",
    )

    add(
        "temporal_rounding",
        [
            f"DATE_TRUNC({kind.upper()} '{value}', {unit})"
            if operation == "trunc"
            else f"{operation.upper()}({kind.upper()} '{value}' TO {unit})"
            for kind, operation, unit, index, value in temporal_rounding_cases()
            if index == 10 and unit in ("YEAR", "MONTH", "WEEK", "DAY", "MILLISECOND")
        ],
        "CREATE VIEW v AS SELECT {e} AS rounded;",
    )

    for target, maximum in (
        ("TINYINT UNSIGNED", 2**8 - 1),
        ("SMALLINT UNSIGNED", 2**16 - 1),
        ("INTEGER UNSIGNED", 2**32 - 1),
        ("BIGINT UNSIGNED", 2**64 - 1),
    ):
        add(
            f"unsigned/{target}",
            [
                f"CAST('{maximum}' AS {target})",
                f"CAST('{maximum + 1}' AS {target})",
                f"CAST(-1 AS {target})",
                f"CAST('-0' AS {target})",
                f"SAFE_CAST('bad' AS {target})",
                f"CAST(TRUE AS {target})",
                f"CAST(NULL AS {target})",
                f"CAST('{maximum}' AS {target}) + CAST(1 AS {target})",
            ],
            "CREATE VIEW v AS SELECT {e} AS y;",
        )

    add(
        "tumble",
        [
            "INTERVAL '1' SECOND",
            "INTERVAL '1' SECOND * 2",
            "INTERVAL '1' SECOND / 2",
            "INTERVAL '0' SECOND",
            "INTERVAL '-1' SECOND",
            "INTERVAL '1' MONTH",
            "INTERVAL '1' SECOND * x",
            "CASE WHEN true THEN INTERVAL '1' SECOND ELSE INTERVAL '1' SECOND / 0 END",
            "CAST(NULL AS INTERVAL SECOND)",
        ],
        "CREATE VIEW v AS SELECT * FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), {e}));",
        needs_input=True,
    )
    add(
        "numeric_interval_tumble",
        ["CAST(1.9 AS INTERVAL DAY)", "CAST(-1.9 AS INTERVAL DAY)",
         "CAST(0.9 AS INTERVAL DAY)", "CAST(1.25 AS INTERVAL SECOND)",
         "CAST(1.9 AS INTERVAL MONTH)", "CAST(0 AS INTERVAL SECOND)"],
        "CREATE VIEW v AS SELECT * FROM TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), {e}));",
        needs_input=True,
    )
    add(
        "post_simplify_warning",
        ["x / (1 - 1)", "x % (1 - 1)", "x / (2 - 1)", "x % 2",
         "CASE WHEN true THEN x ELSE x / 0 END",
         "CASE WHEN false THEN x ELSE x / 0 END"],
        "CREATE VIEW v AS SELECT x, ts, {e} AS y FROM t;",
        needs_input=True,
    )
    for function in ("LEAD", "LAG"):
        add(
            function.lower(),
            [
                "1",
                "1 + 1",
                "CAST('2' AS INTEGER)",
                "ABS(-2)",
                "x",
                "CAST(NULL AS INTEGER)",
                "1 / 0",
                "CASE WHEN true THEN 1 ELSE 1 / 0 END",
            ],
            "CREATE VIEW v AS SELECT "
            + function
            + "(x, {e}) OVER (ORDER BY ts) AS y FROM t;",
            needs_input=True,
        )
        add(
            function.lower() + "_default",
            [
                "0",
                "1 + 1",
                "CAST('2' AS INTEGER)",
                "ABS(-2)",
                "x",
                "CAST(NULL AS INTEGER)",
                "1 / 0",
                "CASE WHEN true THEN 1 ELSE 1 / 0 END",
                "CASE WHEN false THEN 1 ELSE 1 / 0 END",
                "CAST('bad' AS INTEGER)",
                "2147483647 + 1",
                "CAST(1.5 AS INTEGER)",
            ],
            "CREATE VIEW v AS SELECT "
            + function
            + "(x, 1, {e}) OVER (ORDER BY ts) AS y FROM t;",
            needs_input=True,
        )
    for frame in ("ROWS", "RANGE"):
        add(
            frame.lower(),
            ["1", "1 + 1", "ABS(-2)", "-1", "CAST(NULL AS INTEGER)", "1 / 0", "x"],
            "CREATE VIEW v AS SELECT SUM(x) OVER (ORDER BY x "
            + frame
            + " BETWEEN {e} PRECEDING AND CURRENT ROW) AS y FROM t;",
            needs_input=True,
        )
    for clause in ("LIMIT", "OFFSET"):
        add(
            clause.lower(),
            ["1", "(1 + 1)", "CAST('2' AS INTEGER)", "-1", "NULL", "x"],
            "CREATE VIEW v AS SELECT * FROM t " + clause + " {e};",
            needs_input=True,
        )
    add(
        "warnings",
        [
            "CEIL(2::INT)",
            "FLOOR(2::SMALLINT)",
            "ARRAY_COMPACT(ARRAY[1, 2])",
            "ROUND(NULL::DECIMAL(4,2), NULL)",
            "FORMAT_DATE(1151414896, '%Y-%m-%d')",
        ],
        "CREATE VIEW v AS SELECT {e} AS y FROM t;",
        needs_input=True,
    )
    # Keep the searched value dynamic: folding the entire regex call could erase
    # the lowering-site warning before its pattern is inspected.
    regex_patterns = [
        "'a(?=b)'",
        "CONCAT('a', '(?=b)')",
        "'a' || '(?=b)'",
        "CAST('a(?=b)' AS VARCHAR)",
        "CASE WHEN true THEN 'a(?=b)' ELSE CAST(1 / 0 AS VARCHAR) END",
        "CASE WHEN false THEN 'a(?=b)' ELSE 'ab' END",
        "CAST(NULL AS VARCHAR)",
        "CAST(x AS VARCHAR)",
        "'ab'",
        "CONCAT('a', 'b')",
    ]
    add(
        "row_selector",
        [
            "'a'", "CONCAT('a', '')", "'a' || ''", "CAST('a' AS VARCHAR)",
            "CASE WHEN true THEN 'a' ELSE 'missing' END", "CAST(NULL AS VARCHAR)",
            "CAST(x AS VARCHAR)", "'missing'", "1", "1 + 0", "CAST(1 AS INT)",
            "CAST(NULL AS INT)", "x", "0", "3",
        ],
        "CREATE VIEW v AS SELECT x, ts, "
        "CAST(ROW(x, ts) AS ROW(a INT, b TIMESTAMP))[{e}] AS y FROM t;",
        needs_input=True,
    )
    add(
        "keyword_selector",
        [
            "TIMESTAMPDIFF(DAY, ts, ts)",
            "TIMESTAMPDIFF(CONCAT('D', 'AY'), ts, ts)",
            "DATEDIFF(DAY, ts, ts)",
            "DATEDIFF(CAST('DAY' AS VARCHAR), ts, ts)",
            "TIMESTAMP_DIFF(ts, ts, DAY)",
            "TIMESTAMP_DIFF(ts, ts, NULL)",
            "TIMESTAMP_TRUNC(ts, DAY)",
            "TIMESTAMP_TRUNC(ts, CONCAT('D', 'AY'))",
            "FLOOR(ts TO DAY)",
            "FLOOR(ts TO CONCAT('D', 'AY'))",
            "EXTRACT(DAY FROM ts)",
            "TRIM(LEADING 'a' FROM CAST(x AS VARCHAR))",
        ],
        "CREATE VIEW v AS SELECT x, ts, {e} AS y FROM t;",
        needs_input=True,
    )
    add(
        "date_trunc_selector",
        [
            "DATE_TRUNC(ts, DAY)", "DATE_TRUNC('day', ts)",
            "DATE_TRUNC(CONCAT('d', 'ay'), ts)",
            "DATE_TRUNC(CAST('day' AS VARCHAR), ts)",
            "DATE_TRUNC(CASE WHEN true THEN 'day' ELSE 'bad' END, ts)",
            "DATE_TRUNC(CAST(NULL AS VARCHAR), ts)",
            "DATE_TRUNC(CAST(x AS VARCHAR), ts)",
            "DATE_TRUNC(ts, CONCAT('D', 'AY'))",
        ],
        "CREATE VIEW v AS SELECT x, ts, {e} AS y FROM t;",
        needs_input=True,
    )
    for site, expression in [
        ("regex_rlike", "CAST(x AS VARCHAR) RLIKE {e}"),
        ("regex_replace", "REGEXP_REPLACE(CAST(x AS VARCHAR), {e}, 'x')"),
    ]:
        add(
            site,
            regex_patterns,
            "CREATE VIEW v AS SELECT x, ts, " + expression + " AS y FROM t;",
            needs_input=True,
        )
        add(
            site.replace("regex_", "regex_literal_chain_"),
            ["'a'\n'(?=b)'", "'a'\n'b'"],
            "CREATE VIEW v AS SELECT x, ts, " + expression + " AS y FROM t;",
            needs_input=True,
        )
    add(
        "lazy",
        [
            "CASE WHEN true THEN 1 ELSE 1 / 0 END",
            "COALESCE(1, 1 / 0)",
            "CASE WHEN false THEN CAST('bad' AS INTEGER) ELSE 7 END",
            "COALESCE(NULL, CAST('bad' AS INTEGER))",
        ],
        "CREATE VIEW v AS SELECT {e} AS y FROM t;",
        needs_input=True,
    )
    add(
        "interval_cast_unrestricted",
        interval_casts,
        "CREATE VIEW v AS SELECT {e} AS y;",
    )
    add(
        "constant_view",
        [
            *interval_casts,
            "CAST('é' AS VARBINARY)",
            "CAST(X'00ff' AS BINARY(4))",
            "CAST(X'00ff80' AS VARBINARY(2))",
            "SUBSTRING(X'00ff80' FROM 0 FOR 3)",
            "X'00ff' || X'80'",
            "LEFT(X'00ff80', 2)",
            "RIGHT(X'00ff80', 2)",
            "OVERLAY(X'00ff80' PLACING X'aabb' FROM 2 FOR 1)",
            "TO_HEX(X'00ff80')",
            "CAST('123e4567-e89b-12d3-a456-426655440000' AS UUID)",
            "CAST('bad' AS UUID)",
            "TRY_CAST('bad' AS UUID)",
            "CAST(CAST('123e4567e89b12d3a456426655440000' AS UUID) AS VARCHAR)",
            "MD5('Feldera')",
            "OVERLAY('abcdef' PLACING '45' FROM 4)",
            "OVERLAY('éclair' PLACING 'X' FROM 2 FOR 0)",
            "'hawkeye' ILIKE 'H%'",
            "'a%b' LIKE 'a!%_' ESCAPE '!'",
            "ROUND(1.25, 1)",
            "TRUNCATE(-1.29, 1)",
            "ARRAY_DISTINCT(ARRAY[1, NULL, 1, 2])",
            "SUBSTRING('éclair' FROM 0 FOR 3)",
        ],
        "CREATE VIEW v AS SELECT {e} AS y;",
    )
    add(
        "decimal_arithmetic",
        [
            "0.809 * 100",
            "100 * 0.809",
            "1 / CAST(4 AS DECIMAL(12,3))",
            "MOD(-5.25, 2)",
            "MOD(5.25, -2.125)",
            "MOD(-5, 2.125)",
            "MOD(CAST(NULL AS DECIMAL(12,3)), 0)",
            "MOD(5.25, 0)",
        ],
        "CREATE VIEW v AS SELECT {e} AS y;",
    )
    add(
        "value_selection",
        [
            "NULLIF(1, 1)",
            "NULLIF(1, 2)",
            "NULLIF(1, CAST(NULL AS INT))",
            "NULLIF(CAST(NULL AS INT), 1)",
            "NULLIF(1.25, 2.5)",
            "LEAST(2, 1, 3)",
            "GREATEST(2, 1, 3)",
            "LEAST(1, CAST(NULL AS INT))",
            "GREATEST(CAST(NULL AS INT), 1)",
            "LEAST_IGNORE_NULLS(1, CAST(NULL AS INT), -1)",
            "GREATEST_IGNORE_NULLS(CAST(NULL AS INT), 1, 2)",
            "SORT_ARRAY(ARRAY[2, NULL, 1, 2])",
            "SORT_ARRAY(ARRAY[2, NULL, 1, 2], false)",
            "SORT_ARRAY(CAST(NULL AS INT ARRAY))",
            "GREATEST(CAST('NaN' AS DOUBLE), CAST(1 AS DOUBLE))",
            "NULLIF(CAST('NaN' AS DOUBLE), CAST('NaN' AS DOUBLE))",
        ],
        "CREATE VIEW v AS SELECT {e} AS y;",
    )
    integer_metadata = [
        "0",
        "1",
        "-1",
        "1 + 1",
        "ABS(-2)",
        "CAST('2' AS INTEGER)",
        "CAST(NULL AS INTEGER)",
        "1 / 0",
        "x",
        "CASE WHEN true THEN 1 ELSE 1 / 0 END",
        "CAST('bad' AS INTEGER)",
        "2147483647 + 1",
    ]
    add(
        "default_integer",
        integer_metadata,
        "CREATE TABLE metadata_value(y INTEGER DEFAULT {e}); "
        "CREATE VIEW v AS SELECT * FROM metadata_value;",
    )
    add(
        "default_timestamp",
        [
            "TIMESTAMP '2024-02-29 00:00:00'",
            "CAST(NULL AS TIMESTAMP)",
            "TIMESTAMP '2024-01-31 00:00:00' + INTERVAL '1' MONTH",
            "NOW()",
            "CASE WHEN true THEN TIMESTAMP '2024-02-29 00:00:00' ELSE NOW() END",
        ],
        "CREATE TABLE metadata_value(y TIMESTAMP DEFAULT {e}); "
        "CREATE VIEW v AS SELECT * FROM metadata_value;",
    )
    for annotation in ("LATENESS", "WATERMARK"):
        add(
            annotation.lower() + "_integer",
            integer_metadata,
            "CREATE TABLE metadata_value(y INTEGER " + annotation + " {e}); "
            "CREATE VIEW v AS SELECT MAX(y) AS y FROM metadata_value;",
        )
        add(
            annotation.lower() + "_timestamp",
            [
                "INTERVAL '0' SECOND",
                "INTERVAL '1' SECOND",
                "INTERVAL '-1' SECOND",
                "INTERVAL '1' SECOND * 2",
                "INTERVAL '1' SECOND / 0",
                "CAST(NULL AS INTERVAL SECOND)",
                "INTERVAL '1' MONTH",
                "1",
                "CASE WHEN true THEN INTERVAL '1' SECOND ELSE INTERVAL '1' SECOND / 0 END",
            ],
            "CREATE TABLE metadata_value(y TIMESTAMP " + annotation + " {e}); "
            "CREATE VIEW v AS SELECT MAX(y) AS y FROM metadata_value;",
        )
    for (name, program), promote, suppress in itertools.product(
        programs, (False, True), (False, True)
    ):
        settings = []
        if promote:
            settings.append("SET FELDERA_WARNINGS_ARE_ERRORS = true;")
        if suppress:
            for warning in ("NOOP", "UNNECESSARY_FUNCTION_CALL", "SUSPICIOUS_ARGUMENT"):
                settings.append(f"SET FELDERA_IGNORE_WARNING_{warning} = true;")
            if name.startswith("regex_"):
                settings.append(
                    "SET FELDERA_IGNORE_WARNING_UNSUPPORTED_REGULAR_EXPRESSION_FEATURE = true;"
                )
            if name.startswith("post_simplify_warning/"):
                settings.append("SET FELDERA_IGNORE_WARNING_DIVISION_BY_ZERO = true;")
        request = {
            "id": f"focused/{name}/promote={promote}/suppress={suppress}",
            "program": "\n".join([*settings, program]),
        }
        if name.startswith("interval_cast_unrestricted/"):
            # Interval outputs are deliberately forbidden by the public I/O
            # contract. Exercise the folding path as well as that rejection.
            request["options"] = {"languageOptions": {"unrestrictedIOTypes": True}}
        yield request
