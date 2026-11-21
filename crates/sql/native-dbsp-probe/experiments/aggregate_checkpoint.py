#!/usr/bin/env python3
"""Add native accumulator variants to the frozen classic aggregate circuits."""

import argparse
from pathlib import Path


def replace_once(text, original, replacement):
    if text.count(original) != 1:
        raise ValueError("the frozen aggregate closure no longer matches")
    return text.replace(original, replacement, 1)


def fixture(source):
    start = source.index("pub fn circuit0(")
    end = source.index("\npub fn input0(", start)
    circuit = source[start:end]
    replacement = circuit.replace("pub fn circuit0(", "fn circuit0_native(", 1)
    replacement = replace_once(
        replacement,
        "Tup3<i64, i64, i64> {\n                Tup3::new(",
        "native_weights::NativeSum {\n                Tup3::new(",
    )
    replacement = replace_once(
        replacement,
        "                    1i64)\n            },\n"
        "            move |p0: Tup3<i64, i64, i64>, | -> \n"
        "            Tup1<Option<i32>> {\n",
        "                    1i64).into()\n            },\n"
        "            move |p0: native_weights::NativeSum, | -> \n"
        "            Tup1<Option<i32>> {\n"
        "                let p0: Tup3<i64, i64, i64> = p0.into();\n",
    )
    count_start = source.index("pub fn circuit1(")
    count_end = source.index("\npub fn input8(", count_start)
    count = source[count_start:count_end].replace("pub fn circuit1(", "fn circuit1_native(", 1)
    count = replace_once(
        count,
        "Tup2<Tup2<i128, i64>, Tup2<i64, i64>> {\n                Tup2::new(",
        "native_weights::NativeCount {\n                Tup2::new(",
    )
    count = replace_once(
        count,
        "STATIC_6f30b55f0892dfe5.clone())\n            },\n"
        "            move |p0: Tup2<Tup2<i128, i64>, Tup2<i64, i64>>, | -> \n"
        "            Tup2<i64, i64> {\n",
        "STATIC_6f30b55f0892dfe5.clone()).into()\n            },\n"
        "            move |p0: native_weights::NativeCount, | -> \n"
        "            Tup2<i64, i64> {\n"
        "                let p0 = p0.into_reference();\n",
    )
    # The probe already supplies the output helpers and uses the default
    # allocator. Drop only the generated standalone test setup.
    allocator_start = source.index('#[cfg(not(target_env = "msvc"))]\n#[global_allocator]')
    allocator_end = source.index("pub mod stubs;", allocator_start)
    source = source[:allocator_start] + source[allocator_end:]
    source = replace_once(source, "#[cfg(test)]\nuse readers::*;\n", "")
    return (
        source + '\n#[path = "native_weights.rs"]\nmod native_weights;\n' + replacement + count
        + '\n#[cfg(test)]\nmod aggregate_checkpoint;\n'
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("classic", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.write_text(fixture(args.classic.read_text()))


if __name__ == "__main__":
    main()
