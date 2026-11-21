#!/usr/bin/env python3
"""Source audit of explicit frontend simplifier calls; not a reachability proof.

The lexical scan deliberately reports new calls for review. It cannot discover
inherited Calcite methods or prove that a fixture reaches a particular branch.
"""

import argparse
import hashlib
import json
from pathlib import Path
import re

from fold_cases import sql_cases


SRC = Path("sql-to-dbsp-compiler/SQL-compiler/src")
PACKAGE = Path("java/org/dbsp/sqlCompiler")
JAVA = SRC / "main" / PACKAGE
FRONTEND = JAVA / "compiler/frontend"
TEST_FRONTEND = SRC / "test" / PACKAGE / "compiler/frontend"
EXPLICIT_CALL = re.compile(r"\bnew Simplify\s*\(|\.reduce\s*\(")

# (file, unique source anchor, owner, observation, verdict relevance, fixture prefixes)
SITES = [
    ("TableContents.java", "Simplify simplify = new Simplify(compiler);", "table contents", "DBSP Simplify",
     "May fold input literals and emit warnings; no dedicated insertion witness", []),
    ("CalciteToDBSPCompiler.java", "Simplify simplify = new Simplify(this.compiler.compiler());", "TUMBLE size", "DBSP Simplify",
     "Hard positive non-null interval-literal guard after simplification", ["tumble/", "numeric_interval_tumble/"]),
    ("CalciteToDBSPCompiler.java", "caster.reduce(this.compiler)", "output row cast", "DBSPExpression.reduce",
     "Structural cast reduction; no hard constant guard at this call", []),
    ("aggregates/RangeAggregates.java", "Simplify simplify = new Simplify(this.compiler.compiler());", "window bound", "DBSP Simplify",
     "Hard positive non-null numeric/interval-literal guard", ["rows/", "range/"]),
    ("aggregates/RangeAggregates.java", "convertToSigned.call(numericBound).reduce(eComp.compiler())", "signed window bound", "DBSPExpression.reduce",
     "Runs after the literal guard; reduces generated conversion closure", ["range/"]),
    ("aggregates/RangeAggregates.java", "convertToSigned.call(originalOrderField).reduce(this.compiler.compiler())", "window ordering conversion", "DBSPExpression.reduce",
     "Generated dynamic ordering expression; no hard constant guard", ["range/"]),
    ("aggregates/AggregateCompiler.java", "Simplify simplify = new Simplify(this.compiler);", "linear SUM map", "DBSP Simplify",
     "Simplifies generated filter/null-handling expressions; no hard constant guard", []),
    ("calciteCompiler/optimizer/ReduceExpressionsRule.java", "executor.reduce(simplify.rexBuilder, constExps2, reducedValues)", "planner reduction", "observed executor",
     "Can affect all downstream literal consumers and warnings", ["tumble/", "rows/", "range/", "regex_"]),
    ("calciteCompiler/optimizer/FoldTrace.java", "executor.reduce(builder, expressions, output)", "journal wrapper", "observed executor",
     "Records delegated executor results without replacing them", []),
]

RUST_WORKER = ("calciteCompiler/optimizer/RustFoldExecutor.java", "this.reduce(builder, expression, temporalPass)",
               "Rust worker dispatch", "observed replacement",
               "Test worker for planner and selected temporal reductions", [])

PRE_EXECUTOR = [
    ("calciteCompiler/FelderaRexBuilder.java", "return super.makeCast(", "literal casts",
     "Inherited builder may convert a literal before the executor; keepCast preserves known runtime-sensitive casts",
     ["interval_cast_unrestricted/", "constant_view/", "unsigned/"]),
    ("calciteCompiler/ConvertletTable.java", "sourceValue = sourceValue.setScale(0, RoundingMode.DOWN);", "numeric to interval",
     "Numeric SQL literals are truncated toward zero for non-second interval units before literal construction",
     ["numeric_interval_tumble/"]),
    ("calciteCompiler/ConvertletTable.java", "SqlLiteralChainOperator.concatenateOperands(call)", "adjacent string literals",
     "SQL literal-chain concatenation can expose a regex warning before any executor reduction",
     ["regex_literal_chain_"]),
]


def unclassified_calls(root, sites=SITES, pattern="**/*.java"):
    """Find lexical calls that have no reviewed file/anchor pair."""
    unmatched = []
    for path in sorted(root.glob(pattern)):
        relative = str(path.relative_to(root))
        for line, text in enumerate(path.read_text().splitlines(), 1):
            if EXPLICIT_CALL.search(text) and not any(
                relative == site[0] and site[1] in text for site in sites
            ):
                unmatched.append({"file": relative, "line": line, "text": text.strip()})
    return unmatched


def inventory(repository):
    fixtures = list(sql_cases())
    sources = {}
    sites = []
    missing = []
    entries = [(FRONTEND, site) for site in SITES]
    entries.append((TEST_FRONTEND, RUST_WORKER))
    entries.extend((FRONTEND, (file, anchor, owner, "before executor", relevance, prefixes))
                   for file, anchor, owner, relevance, prefixes in PRE_EXECUTOR)
    for directory, (file, anchor, owner, observation, relevance, prefixes) in entries:
        relative = directory / file
        source = (repository / relative).read_text()
        sources[str(relative)] = hashlib.sha256(source.encode()).hexdigest()
        lines = [i for i, text in enumerate(source.splitlines(), 1) if anchor in text]
        if len(lines) != 1:
            missing.append({"file": file, "anchor": anchor, "matches": len(lines)})
        sites.append({"owner": owner, "file": str(relative), "lines": lines,
                      "anchor": anchor, "observation": observation, "relevance": relevance,
                      "fixture_ids": [c["id"] for c in fixtures if any(
                          c["id"].startswith("focused/" + prefix) for prefix in prefixes)],
                      "branch_reachability_proven": False})
    unknown = unclassified_calls(repository / FRONTEND)
    unknown.extend(unclassified_calls(repository / TEST_FRONTEND, [RUST_WORKER], RUST_WORKER[0]))
    for relative in (JAVA / "ir/expression/DBSPExpression.java",
                     JAVA / "compiler/visitors/inner/Simplify.java",
                     JAVA / "compiler/visitors/outer/CircuitOptimizer.java"):
        sources[str(relative)] = hashlib.sha256((repository / relative).read_bytes()).hexdigest()
    return {"schema_version": 1, "sources": sources, "sites": sites,
            "unclassified_calls": unknown, "missing_or_ambiguous_anchors": missing,
            "local_explicit_frontend_calls_classified": not unknown and not missing,
            "all_constant_site_coverage_complete": False,
            "outside_scan": ["Inherited Calcite validation, RexBuilder and RexSimplify internals",
                             "DBSPExpression.reduce: beta reduction followed by bounded DBSP Simplify",
                             "Mid-end pass driver and Simplify internals, including warning emission",
                             "Dynamically dispatched calls and alternative lexical spellings"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args()
    result = inventory(args.repository)
    print(json.dumps(result, indent=2))
    if not result["local_explicit_frontend_calls_classified"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
