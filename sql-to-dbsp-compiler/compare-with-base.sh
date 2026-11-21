#!/usr/bin/env bash
# Compare the files that the pipeline manager receives from each compiler.
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
cd "$repo"
if (( $# > 1 )); then
    echo "Usage: $0 [<base-ref>]" >&2
    exit 2
fi
base=${1:-$(git merge-base HEAD origin/main)}
work=$(mktemp -d "${TMPDIR:-/tmp}/compiler-parity.XXXXXXXX")
trap 'rm -rf "$work"' EXIT

compiler=sql-to-dbsp-compiler
mkdir "$work/base"
git archive "$base" "$compiler" | tar -x -C "$work/base"
for tree in "$work/base" "$repo"; do
    (cd "$tree/$compiler/SQL-compiler" && mvn -q -DskipTests package)
done
jar=$compiler/SQL-compiler/target/sql2dbsp-jar-with-dependencies.jar
python3 "$compiler/generate-parity-programs.py" "$work/programs"
java=${JAVA_HOME:+$JAVA_HOME/bin/}java

failed=0
for program in "$work"/programs/*.sql; do
    name=$(basename "$program" .sql)
    for mode in classic gen2; do
        for version in base current; do
            tree=$repo
            if [[ $version == base ]]; then tree=$work/base; fi
            out=$work/$version-out
            mkdir "$out"
            args=(-js schema.json --dataflow dataflow.json -i -je --alltables --ignoreOrder --enterprise)
            if [[ $mode == classic ]]; then
                mkdir "$out/rust"
                args+=(-o rust --runtime "$repo/crates" --crates pipeline_x)
            else
                args+=(--gen2 -o gen2.json)
            fi
            status=0
            (cd "$out" && "$java" -jar "$tree/$jar" "$program" "${args[@]}" 2>stderr) || status=$?
            echo "$status" > "$out/status"
            if (( status == 0 )); then
                for file in schema.json dataflow.json; do
                    if [[ ! -s "$out/$file" ]]; then
                        echo "$name ($mode, $version) is missing $file" >&2
                        failed=1
                    fi
                done
                if [[ $mode == classic ]]; then artifact=rust/Cargo.toml; else artifact=gen2.json; fi
                if [[ ! -s "$out/$artifact" ]]; then
                    echo "$name ($mode, $version) is missing $artifact" >&2
                    failed=1
                fi
            elif (( status != 1 )) || ! python3 -c '
import json, sys
messages = json.load(open(sys.argv[1]))
sys.exit(not any(message.get("warning") is False for message in messages))
' "$out/stderr"; then
                echo "$name ($mode, $version) exited with $status without a SQL diagnostic" >&2
                failed=1
            fi
        done
        if ! diff -qr "$work/base-out" "$work/current-out" > "$work/differences"; then
            while IFS= read -r difference; do
                printf '%s (%s) %s\n' "$name" "$mode" "$difference" >&2
            done < "$work/differences"
            failed=1
        fi
        rm -r "$work/base-out" "$work/current-out"
    done
done
exit "$failed"
