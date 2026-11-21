"""Deterministic adversarial inputs for the P0 connector decoder prototype."""

import itertools
import json

BASES = {
    "PostgresReaderConfig": {"uri": "postgres://example/db", "query": "SELECT 1"},
    "KafkaInputConfig": {"topic": "events"},
    "IcebergReaderConfig": {
        "mode": "snapshot",
        "metadata_location": "s3://bucket/meta",
    },
    "AvroParserConfig": {},
    "AvroEncoderConfig": {},
    "HttpOutputConfig": {},
}


def cases(inventory):
    """Select every case before running either implementation; never filter failures."""
    records = []

    def add(name, label, config, *, pretty=False):
        document = [{"name": "probe 🦀", "transport": {"config": config}}]
        records.append(
            {
                "id": f"{name}/{label}",
                "config_class": name,
                "json": json.dumps(
                    document, ensure_ascii=False, indent=2 if pretty else None
                ),
                "pointer": "/0/transport/config",
            }
        )

    values = [
        None,
        False,
        True,
        -1,
        0,
        1,
        2,
        2147483647,
        2147483648,
        -2147483649,
        9223372036854775807,
        9223372036854775808,
        18446744073709551615,
        1.5,
        -0.5,
        "",
        " ",
        "0",
        "1",
        "-1",
        "1.5",
        "true",
        "TRUE",
        "false",
        "null",
        "bad",
        "🦀",
        [],
        [None],
        [1, "x"],
        {},
        {"a": None},
        {"a": []},
        {"a/b~🦀": {}},
        "\u2003",
        "\u00a0",
        "O'Reilly",
        "line\n\tvalue",
        1.0,
        0.0,
        2147483647.5,
        -2147483648.5,
        1e20,
        1e-20,
        " 1 ",
        "+1",
        "01",
        "True",
        "False",
        "TrUe",
        "NULL",
        " raw ",
        "\u001c",
        {"a": [None]},
        [1, {}],
    ]
    for schema in inventory["classes"]:
        name = schema["class"]
        if name not in BASES:
            continue
        base = BASES[name]
        add(name, "empty", {})
        add(name, "valid", base)
        for field in schema["fields"]:
            key = field["name"]
            for index, value in enumerate(values + field.get("enum_values", [])):
                add(name, f"{key}/{index}", base | {key: value}, pretty=index % 2 == 0)
            add(name, f"missing/{key}", {k: v for k, v in base.items() if k != key})
        for key in ["unknown", "a/b~c", "🦀", "registry"]:
            for index, value in enumerate(values):
                add(name, f"unknown/{key}/{index}", base | {key: value})
        # First deserialization error follows input order, not sorted field order.
        add(name, "two-errors", {"first_unknown": 1, **base, "last_unknown": 2})

    for topic, topics in itertools.product(
        [None, "", " ", "events"], [None, [], ["a"], ["a", "b"], [None]]
    ):
        add(
            "KafkaInputConfig",
            f"topics/{len(records)}",
            {"topic": topic, "topics": topics},
        )
    for name in ["AvroParserConfig", "AvroEncoderConfig"]:
        for schema, urls, token, user in itertools.product(
            [None, "schema"], [None, [], ["url"]], [None, "token"], [None, "user"]
        ):
            add(
                name,
                f"registry/{len(records)}",
                {
                    "schema": schema,
                    "registry_urls": urls,
                    "registry_authorization_token": token,
                    "registry_username": user,
                },
            )
        for update, cdc in itertools.product(
            [None, "raw", "debezium", "confluent_jdbc"], [None, "op"]
        ):
            if name == "AvroEncoderConfig":
                add(
                    name,
                    f"cdc/{len(records)}",
                    {"update_format": update, "cdc_field": cdc},
                )
    for catalog, metadata, table in itertools.product(
        [None, "glue", "rest", "s3tables"], [None, "meta"], [None, "table"]
    ):
        for props in [
            {},
            {"glue.warehouse": "w", "rest.uri": "u", "s3tables.table-bucket-arn": "a"},
        ]:
            add(
                "IcebergReaderConfig",
                f"catalog/{len(records)}",
                {
                    "mode": "snapshot",
                    "catalog_type": catalog,
                    "metadata_location": metadata,
                    "table_name": table,
                    "snapshot_id": 1,
                    "datetime": "date",
                    **props,
                },
                pretty=True,
            )
    durations = [
        None,
        0,
        1.0,
        1.5,
        -1,
        18446744073709551615,
        18446744073709551616,
        "0",
        "+0",
        "",
        "+",
        "-1s",
        "1s0",
        "1h30m",
        "0.5ns",
        "1μs",
        "1µs",
        "1..5s",
        ".5s",
        "5.s",
        "s",
        "forever",
        "1 s",
        "1e3s",
        "1S",
        "18446744073709551615s999999999ns",
        "18446744073709551616s",
        [],
        {},
    ]
    for name, current, legacy in [
        ("KafkaInputConfig", "group_join_timeout", "group_join_timeout_secs"),
        ("AvroParserConfig", "registry_timeout", "registry_timeout_secs"),
        ("AvroEncoderConfig", "registry_timeout", "registry_timeout_secs"),
    ]:
        for field, value in itertools.product([current, legacy], durations):
            add(name, f"duration/{len(records)}", BASES[name] | {field: value})
        for left, right in itertools.product([None, 1, "1s"], repeat=2):
            add(
                name,
                f"aliases/{len(records)}",
                BASES[name] | {current: left, legacy: right},
            )
    return records


def sql_cases(records):
    """Run every selected input through SQL with all four SET policy combinations."""
    modes = {
        "PostgresReaderConfig": ("transport", "postgres_input", True),
        "KafkaInputConfig": ("transport", "kafka_input", True),
        "IcebergReaderConfig": ("transport", "iceberg_input", True),
        "AvroParserConfig": ("format", "avro", True),
        "AvroEncoderConfig": ("format", "avro", False),
        "HttpOutputConfig": ("transport", "http_output", False),
    }
    policies = {
        "enabled": "",
        "suppressed": "SET FELDERA_IGNORE_WARNING_INVALID_CONFIGURATION = true;\n",
        "promoted": "SET FELDERA_WARNINGS_ARE_ERRORS = true;\n",
        "suppressed_and_promoted": "SET FELDERA_IGNORE_WARNING_INVALID_CONFIGURATION = true;\nSET FELDERA_WARNINGS_ARE_ERRORS = true;\n",
    }
    for record in records:
        kind, name, is_input = modes[record["config_class"]]
        config = json.loads(record["json"])[0]["transport"]["config"]
        raw = json.dumps(
            [{"name": "probe 🦀", kind: {"name": name, "config": config}}],
            ensure_ascii=False,
            indent=2 if "\n" in record["json"] else None,
        )
        for policy, statements in policies.items():
            prefix = statements + (
                "CREATE TABLE t(x INT) WITH (\n  'connectors' = '"
                if is_input
                else "CREATE TABLE t(x INT);\nCREATE VIEW v WITH (\n  'connectors' = '"
            )
            suffix = (
                "');\nCREATE VIEW v AS SELECT * FROM t;\n"
                if is_input
                else "') AS SELECT * FROM t;\n"
            )
            yield {
                "id": record["id"] + "/" + policy,
                "config_class": record["config_class"],
                "json": raw,
                "pointer": f"/0/{kind}/config",
                "source": {
                    "program": prefix + raw.replace("'", "''") + suffix,
                    "line": prefix.count("\n") + 1,
                    "column": len(prefix.rsplit("\n", 1)[-1].encode("utf-16-le")) // 2,
                    "policy": policy,
                },
            }


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    with args.output.open("x") as output:
        for case in cases(json.loads(args.inventory.read_text())):
            output.write(json.dumps(case, ensure_ascii=False) + "\n")
