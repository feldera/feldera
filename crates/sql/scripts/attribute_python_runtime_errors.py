#!/usr/bin/env python3
"""Attribute the 61 observed Python runtime errors to reviewed formatter roots.

This bounded catalog is not a source-wide template census. Macro instances share
one reviewed root; transparent wrappers and external formatters remain separate.
"""

import argparse
import hashlib
import json
import re
from pathlib import Path


ROOTS = {
    "plus": ("crates/sqllib/src/operators.rs", "fn plus<T>", "'{left} + {right}' causes overflow for type {sql_type}"),
    "minus": ("crates/sqllib/src/operators.rs", "fn minus<T>", "'{left} - {right}' causes overflow for type {sql_type}"),
    "times": ("crates/sqllib/src/operators.rs", "fn times<T>", "'{left} * {right}' causes overflow for type {sql_type}"),
    "div": ("crates/sqllib/src/operators.rs", "fn div<T>", "'{left} / {right}' causes overflow for type {sql_type}"),
    "decimal_string": ("crates/sqllib/src/casts.rs", "pub fn cast_to_SqlDecimal_s<const", "Error converting '{value}' to DECIMAL({P}, {S}): {decimal_parse_error}"),
    "decimal_real": ("crates/sqllib/src/casts.rs", "pub fn cast_to_SqlDecimal_f<const", "Error converting {value} to DECIMAL({P}, {S}): {decimal_range_error}"),
    "uuid_binary": ("crates/sqllib/src/casts.rs", "pub fn cast_to_Uuid_bytes(", "Need exactly 16 bytes in BINARY value to create an UUID"),
    "time_string": ("crates/sqllib/src/casts.rs", "pub fn cast_to_Time_s(", "Error converting {value} to TIME: {chrono_parse_error}"),
    "uuid_string": ("crates/sqllib/src/uuid.rs", "pub fn try_from_ref(", "Invalid UUID string '{value}'"),
    "integer_narrowing": ("crates/sqllib/src/casts.rs", "macro_rules! cast_to_i_i", "Error converting {value} to {sql_type}: {TryFromIntError}"),
}

DEPENDENCIES = {
    "sql_type_name": ("crates/sqllib/src/casts.rs", "pub(crate) fn type_name("),
    "integer_operator_instantiations": ("crates/sqllib/src/operators.rs", "macro_rules! for_all_int_operator"),
    "nullable_operator_wrappers": ("crates/sqllib/src/lib.rs", "macro_rules! some_operator"),
    "nullable_cast_wrappers": ("crates/sqllib/src/casts.rs", "macro_rules! cast_function"),
    "nullable_decimal_string": ("crates/sqllib/src/casts.rs", "pub fn cast_to_SqlDecimalN_sN<const"),
    "nullable_decimal_real": ("crates/sqllib/src/casts.rs", "pub fn cast_to_SqlDecimalN_fN<const"),
    "decimal_parse_error": ("crates/fxp/src/lib.rs", "impl std::fmt::Display for ParseDecimalError"),
    "decimal_range_error": ("crates/fxp/src/lib.rs", "impl std::fmt::Display for OutOfRange"),
    "decimal_real_conversion": ("crates/fxp/src/fixed.rs", "impl<const P: usize, const S: usize> TryFrom<f64>"),
    "remainder_callsite": ("crates/sqllib/src/operators.rs", "fn modulo<T>"),
    "uuid_string_cast": ("crates/sqllib/src/casts.rs", "pub fn cast_to_Uuid_s("),
}

WRAPPERS = {
    "sql_result_display": ("crates/sqllib/src/error.rs", "pub enum SqlRuntimeError"),
    "source_position_panic": ("crates/sqllib/src/casts.rs", "pub fn handle_error_with_position<T>"),
    "source_position_display": ("crates/sqllib/src/source.rs", "impl fmt::Display for SourcePosition {"),
    "worker_panic_display": ("crates/dbsp/src/circuit/runtime.rs", "impl Display for WorkerPanicInfo"),
    "worker_runtime_error_display": ("crates/dbsp/src/circuit/runtime.rs", "impl Display for Error {"),
    "dbsp_error_display": ("crates/dbsp/src/error.rs", "impl Display for Error {"),
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def locate(path, anchor):
    text = path.read_text()
    if text.count(anchor) != 1:
        raise ValueError(f"reviewed source anchor changed: {path}: {anchor}")
    return {"anchor": anchor, "line": text[:text.index(anchor)].count("\n") + 1,
            "sha256": digest(path)}


def classify(identity):
    name = identity.rsplit(":", 1)[1]
    special = {"illarg_sum_illegal1": ("decimal_string", r"cast_to_SqlDecimalN_sN"),
               "illarg_cast_real_illegal": ("decimal_real", r"cast_to_SqlDecimalN_fN"),
               "illarg_cast_binary_illegal": ("uuid_binary", r"cast_to_UuidN_bytesN"),
               "illarg_cast_array_illegal": ("time_string", r"cast_to_TimeN_sN"),
               "illarg_union_all_illegal": ("uuid_string", r"cast_to_Uuid_s")}
    if name in special:
        return special[name]
    for prefix, root in [("neg_add_", "plus"), ("neg_sub_", "minus"), ("neg_mul_", "times"),
                         ("neg_div_", "div"), ("neg_mod_", "rust_remainder_zero")]:
        if name.startswith(prefix):
            function = "modulo" if root == "rust_remainder_zero" else root
            return root, function + r"_[iu](?:8|16|32|64)N?_[iu](?:8|16|32|64)N?"
    if name.startswith("neg_sum_"):
        return "integer_narrowing", r"cast_to_[iu](?:8|16|32|64)N_(?:i64|i128)"
    raise ValueError(f"runtime case has no reviewed attribution: {identity}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("certificate", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--java-sources", required=True, type=Path)
    args = parser.parse_args()
    certificate = json.loads(args.certificate.read_text())

    def artifact(relative):
        path = args.run / relative
        if certificate["artifacts"].get(args.run.name + "/" + relative) != digest(path):
            raise ValueError(f"runtime artifact differs from certificate: {relative}")
        return path

    def source(row):
        relative, anchor = row
        path = args.repo / relative
        if certificate["source_sha256"].get(relative) != digest(path):
            raise ValueError(f"runtime source differs from execution snapshot: {relative}")
        return dict(locate(path, anchor), path=relative)

    roots = {name: dict(source((path, anchor)), format=template, ownership="repository", cases=[])
             for name, (path, anchor, template) in ROOTS.items()}
    roots["rust_remainder_zero"] = {
        "ownership": "Rust toolchain", "symbol": "core::panicking::panic_const::panic_const_rem_by_zero",
        "format": "attempt to calculate the remainder with a divisor of zero", "source_body_available": False, "cases": []}
    for name in ["plus", "minus", "times", "div"]:
        roots[name]["dependencies"] = ["sql_type_name", "integer_operator_instantiations", "nullable_operator_wrappers"]
    roots["decimal_string"]["dependencies"] = ["nullable_decimal_string", "decimal_parse_error"]
    roots["decimal_real"]["dependencies"] = ["nullable_decimal_real", "decimal_real_conversion", "decimal_range_error"]
    roots["uuid_binary"]["dependencies"] = ["nullable_cast_wrappers"]
    roots["uuid_string"]["dependencies"] = ["uuid_string_cast"]
    roots["time_string"]["dependencies"] = ["nullable_cast_wrappers", "chrono_parse_error"]
    roots["integer_narrowing"]["dependencies"] = ["sql_type_name", "rust_try_from_int_error"]
    roots["rust_remainder_zero"]["dependencies"] = ["remainder_callsite", "integer_operator_instantiations", "nullable_operator_wrappers", "rust_toolchain"]
    records = [json.loads(line) for line in artifact("results.jsonl").read_text().splitlines()]
    if len(records) != 61 or any(not row["local_runtime_contract_matched"] for row in records):
        raise ValueError("expected the complete successful 61-case runtime population")
    attributions = []
    for index, result in enumerate(records):
        case = json.loads(artifact(f"cases/{index:03d}.json").read_text())
        if case["id"] != result["id"]:
            raise ValueError("result/case order differs")
        raw = artifact(f"src/case_{index:03d}/mod.raw.rs")
        generated = raw.read_text()
        root, pattern = classify(case["id"])
        calls = sorted(set(re.findall(pattern + r"\b", generated)))
        if len(calls) != 1:
            raise ValueError(f"expected one reviewed generated helper for {case['id']}: {calls}")
        error = result["observation"]["observation"]["error"]
        messages = re.findall(r"panic message: ([^\n]+)", error)
        locations = re.findall(r"panic location: ([^\n]+)", error)
        if len(messages) != 1 or len(locations) != 1 or case["expected"].strip().lower() not in messages[0].lower():
            raise ValueError("observed panic does not match the attributed expectation")
        if root == "rust_remainder_zero" and roots[root]["symbol"] not in error:
            raise ValueError("remainder stack does not identify the delegated Rust formatter")
        roots[root]["cases"].append(case["id"])
        attributions.append({"id": case["id"], "root": root, "generated_helper": calls[0],
                             "generated_source_sha256": digest(raw), "expected": case["expected"],
                             "observed_panic_message": messages[0], "observed_panic_location": locations[0],
                             "position_wrapper": "source_position_panic" if "handle_error_with_position" in generated else None})
    cargo = json.loads(artifact("cargo-metadata.json").read_text())
    chrono = next(package for package in cargo["packages"] if package["name"] == "chrono")
    chrono_path = Path(chrono["manifest_path"]).parent / "src/format/mod.rs"
    chrono_source = dict(locate(chrono_path, "impl fmt::Display for ParseError"), path=str(chrono_path),
                         package_version=chrono["version"], observed_variant="ParseErrorKind::Invalid")
    java_relative = "org/dbsp/sqlCompiler/ir/expression/DBSPHandleErrorExpression.java"
    java = args.java_sources / java_relative
    if certificate["source_sha256"].get("frozen-java/main/" + java_relative) != digest(java):
        raise ValueError("generator source differs from compiler snapshot")
    generator = dict(locate(java, "public String getFunction()"),
                     path="frozen-java/main/" + java_relative,
                     role="PanicWithSource selects handle_error_with_position; not an additional error-body template")
    report = {"schema_version": 1, "complete": True, "scope": "61 Python runtime cases only; not a runtime source-wide census",
              "execution_certificate_sha256": digest(args.certificate), "attribution_tool_sha256": digest(Path(__file__)),
              "counts": {"cases": 61, "repository_error_body_roots": len(ROOTS), "delegated_error_body_roots": 1,
                         "shared_repository_wrappers": len(WRAPPERS)},
              "error_body_roots": roots, "cases": attributions,
              "shared_wrappers": {name: source(row) for name, row in WRAPPERS.items()},
              "dependencies": {name: source(row) for name, row in DEPENDENCIES.items()},
              "external_dependencies": {"chrono_parse_error": chrono_source,
                                        "rust_try_from_int_error": {"symbol": "core::num::error::TryFromIntError::fmt", "observed_text": "number too large to fit in target type", "source_body_available": False},
                                        "rust_primitive_display": {"role": "integer and floating operand rendering", "source_body_available": False},
                                        "dependency_versions": {name: sorted({package["version"] for package in cargo["packages"] if package["name"] == name}) for name in ["chrono", "num-traits", "thiserror"]},
                                        "rust_toolchain": json.loads(artifact("environment.json").read_text())["rustc"]},
              "generator_wrapper_selection": generator,
              "counting_rule": "one reviewed source function or macro body per error root; concrete type/nullability instances and shared wrappers do not multiply error-body roots",
              "normal_suite_executes_negative_views": False, "platform_transport_executed": False,
              "full_runtime_template_inventory_complete": False, "P0_complete": False}
    with args.output.open("x") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(json.dumps({"counts": report["counts"], "root_case_counts": {name: len(row["cases"]) for name, row in roots.items()}}))


if __name__ == "__main__":
    main()
