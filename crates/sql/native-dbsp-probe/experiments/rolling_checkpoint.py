#!/usr/bin/env python3
"""Produce a fixture with the unchanged classic circuit and native encoded join keys."""

import argparse
from pathlib import Path


def replace_once(source, original, replacement):
    if source.count(original) != 1:
        raise ValueError(f"unexpected classic rolling fixture: {original}")
    return source.replace(original, replacement, 1)


def fixture(source):
    start = source.index("pub fn circuit0(")
    end = source.index("\npub fn input0(", start)
    native = source[start:end].replace("pub fn circuit0(", "fn circuit0_native(", 1)
    start = native.index("let s4: Stream<")
    end = native.index("s6.set_persistent_id", start)
    join = native[start:end]
    key_type = "Tup1<Option<SqlDecimal<6, 2>>>"
    if join.count(key_type) != 5:
        raise ValueError("expected two indexed streams and the join key parameter")
    join = join.replace(key_type, "NativeDecimal")
    join = replace_once(
        join,
        "(*p0.1).0, true, false))))),",
        "(*p0.1).0, true, false))))).into(),",
    )
    join = replace_once(
        join,
        "Tup1::new((*p0).1.as_ref().cloned()),",
        "Tup1::new((*p0).1.as_ref().cloned()).into(),",
    )
    native = native[:start] + join + native[end:]
    # Leave the original constructor and input bags byte for byte. The parent
    # harness provides the allocator, empty UDF module and stubs.
    start = source.index('#[cfg(not(target_env = "msvc"))]\n#[global_allocator]')
    end = source.index("#[cfg(test)]\nuse readers::*;", start)
    source = source[:start] + source[end:]
    return (
        source
        + '\n#[path = "native_decimal.rs"]\nmod native_decimal;\n'
        + 'use native_decimal::NativeDecimal;\n'
        + native
        + '\n#[path = "rolling_checkpoint.rs"]\nmod rolling_checkpoint;\n'
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("classic", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.write_text(fixture(args.classic.read_text()))


if __name__ == "__main__":
    main()
