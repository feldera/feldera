#!/usr/bin/env bash
# Compare the files that the pipeline manager receives from each compiler.
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
compiler=$repo/sql-to-dbsp-compiler
usage() {
    echo "Usage: $0 [BASE_REF] [--base-jar PATH] [--current-jar PATH] [--comparator PATH]" >&2
    exit 2
}
base=
base_jar=
current_jar=
comparator=
while (( $# )); do
    case $1 in
        --base-jar|--current-jar|--comparator)
            [[ $# -ge 2 && -f $2 ]] || usage
            path=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
            case $1 in
                --base-jar) base_jar=$path ;;
                --current-jar) current_jar=$path ;;
                --comparator) comparator=$path ;;
            esac
            shift 2 ;;
        -*) usage ;;
        *) [[ -z $base ]] || usage; base=$1; shift ;;
    esac
done
cd "$repo"
base=${base:-$(awk 'NF && !/^#/ {print $1}' "$compiler/parity-base")}
work=$(mktemp -d "${TMPDIR:-/tmp}/compiler-parity.XXXXXXXX")
trap 'rm -rf "$work"' EXIT

jar=SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar
if [[ -z $base_jar ]]; then
    git archive "$base" sql-to-dbsp-compiler | tar -x -C "$work"
    (cd "$work/sql-to-dbsp-compiler/SQL-compiler" && mvn -q -DskipTests package)
    base_jar=$work/sql-to-dbsp-compiler/$jar
fi
if [[ -z $current_jar ]]; then
    (cd "$compiler/SQL-compiler" && mvn -q -DskipTests package)
    current_jar=$compiler/$jar
fi
if [[ -z $comparator ]]; then
    cargo build --locked --release -p feldera-sql-difftest --bin sql-difftest
    target_dir=$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
    comparator=$target_dir/release/sql-difftest
fi
python3 "$compiler/generate-parity-programs.py" "$work/programs"
python3 - "$work" "$base_jar" "$current_jar" "$comparator" "${JAVA_HOME:+$JAVA_HOME/bin/}java" <<'PY'
from concurrent.futures import ThreadPoolExecutor
import filecmp
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

work, base, current, comparator = map(Path, sys.argv[1:5])
repo = Path.cwd()
manifest = repo / "sql-to-dbsp-compiler/parity-expected-differences"
java = sys.argv[5]


def diagnostic_stderr(data):
    frame = rb"(?:at +)?[\w.$/@-]+\.[\w$<>]+\((?:[\w.$-]+\.\w+(?::-?\d+)?|Unknown Source|Native Method)\)"
    trace = rb"(?:" + frame + rb"|\.\.\. \d+ more)"
    data = re.sub(rb"(?m)^[ \t]*" + trace + rb"[ \t]*\r?(?:\n|$)", b"", data)
    # JSON messages can contain stack frames with escaped newlines and tabs.
    newline = rb"(?:\\r)?\\n"
    return re.sub(newline + rb"(?:\\t| )*" + trace + rb'(?=' + newline + rb'|")', b"", data)


def compile_program(program, mode, jar, out):
    out.mkdir()
    flags = ["-js", "schema.json", "--dataflow", "dataflow.json", "-i", "-je",
             "--alltables", "--ignoreOrder", "--enterprise"]
    if mode == "classic":
        (out / "rust").mkdir()
        artifact = "rust/Cargo.toml"
        flags += ["-o", "rust", "--runtime", repo / "crates", "--crates", "pipeline_x"]
    else:
        artifact = "gen2.json"
        flags += ["--gen2", "-o", artifact]
    command = [java, "-XX:+UnlockExperimentalVMOptions", "-XX:hashCode=2", "-jar", jar, program, *flags]
    result = subprocess.run(command, cwd=out, stderr=subprocess.PIPE)
    stderr = diagnostic_stderr(result.stderr)
    (out / "stderr").write_bytes(stderr)
    (out / "status").write_text(f"{result.returncode}\n")
    if result.returncode == 0:
        for name in (artifact, "schema.json", "dataflow.json"):
            if not (out / name).is_file() or not (out / name).stat().st_size:
                raise ValueError(f"{program.name} {mode} missing {name}. Check the compiler output.")
    else:
        try:
            errors = json.loads(stderr)
        except ValueError:
            errors = []
        if result.returncode != 1 or not isinstance(errors, list) or not any(
                isinstance(e, dict) and e.get("warning") is False for e in errors):
            raise ValueError(f"{program.name} {mode} status {result.returncode}. Check stderr: {stderr[-2000:]!r}")


def differences(left, right, compare_gen2):
    files = {p.relative_to(root) for root in (left, right) for p in root.rglob("*") if p.is_file()}
    for name in sorted(files):
        a, b = left / name, right / name
        if a.is_file() and b.is_file():
            if compare_gen2 and name == Path("gen2.json"):
                result = subprocess.run([comparator, a, b], capture_output=True, text=True)
                if result.returncode == 0:
                    continue
                # Old variant literals have a duplicate type key. Validate the replacement.
                if result.returncode == 2 and result.stderr.startswith(f"invalid JSON in {a}: duplicate JSON object key"):
                    valid = subprocess.run([comparator, b, b], capture_output=True, text=True)
                    if valid.returncode:
                        raise ValueError(f"Invalid candidate {b}. Check the Gen-2 output: {valid.stderr}")
                elif result.returncode != 1:
                    raise ValueError(f"Cannot compare {a} and {b}. Check sql-difftest: {result.stderr}")
            elif filecmp.cmp(a, b, shallow=False):
                continue
        yield str(name)


def check(case):
    program, mode = case
    with tempfile.TemporaryDirectory(dir=work) as scratch:
        first, repeat, candidate = (Path(scratch) / name for name in ("first", "repeat", "candidate"))
        compile_program(program, mode, base, first)
        compile_program(program, mode, base, repeat)
        for name in differences(first, repeat, False):
            raise ValueError(f"{program.name} {mode} {name} changed between base runs. Check compiler determinism.")
        compile_program(program, mode, current, candidate)
        return {(program.name, mode, name) for name in differences(first, candidate, True)}


def main():
    expected = set()
    for number, line in enumerate(manifest.read_text().splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split(maxsplit=3)
        if len(fields) != 4 or tuple(fields[:3]) in expected:
            raise ValueError(f"Invalid entry on line {number} of {manifest.name}. Specify one difference and its reason.")
        expected.add(tuple(fields[:3]))
    cases = [(p, mode) for p in sorted((work / "programs").glob("*.sql"))
             for mode in ("classic", "gen2")]
    observed = set()
    with ThreadPoolExecutor(max_workers=4) as workers:
        for result in workers.map(check, cases):
            observed.update(result)
    for difference in sorted(observed - expected):
        print(*difference, "differs. Review the compiler change.", file=sys.stderr)
    for difference in sorted(expected - observed):
        print(*difference, "now matches. Review the expected difference.", file=sys.stderr)
    print(f"Compared {len(cases)} program and mode pairs with {len(observed)} differences.")
    return int(expected != observed)


try:
    sys.exit(main())
except (ValueError, OSError) as error:
    print(error, file=sys.stderr)
    sys.exit(2)
PY
