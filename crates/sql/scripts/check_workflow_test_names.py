#!/usr/bin/env python3
"""Check shell test selectors in GitHub workflows against the checked-out sources."""
from pathlib import Path
import ast
import re
import shlex
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[3]
RUST_DECL = re.compile(r"^(?P<indent> *)(?:mod (?P<module>\w+)\s*\{|(?P<close>\})|"
                       r"#\[(?:[\w:]*::)?(?:test|engine_test)\b[^\]]*\]\s*"
                       r"(?:#\[[^\]]*\]\s*)*(?:pub\s+)?(?:async\s+)?fn\s+(?P<test>\w+))", re.M)
JAVA_METHOD = re.compile(r"@Test\b[^{};]*?\bvoid\s+(\w+)\s*\(")


def sources():
    paths = subprocess.check_output(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "*.rs", "*.java"], cwd=ROOT)
    java, rust = {}, set()
    for raw in paths.split(b"\0"):
        if not raw:
            continue
        path = Path(raw.decode())
        if not (ROOT / path).is_file():
            continue
        if path.suffix == ".java":
            if "test" in path.parts:
                source = (ROOT / path).read_text()
                methods = JAVA_METHOD.findall(source)
                if methods:
                    java.setdefault(path.stem, set()).update(methods)
                    package = re.search(r"^package ([\w.]+);", source, re.M)
                    if package:
                        java[f"{package[1]}.{path.stem}"] = set(methods)
            continue
        text = (ROOT / path).read_text()
        parts = path.with_suffix("").parts
        for root in ("src", "tests"):
            if root in parts:
                parts = parts[parts.index(root) + 1:]
                break
        if parts and parts[-1] in {"lib", "main", "mod"}:
            parts = parts[:-1]
        parents = {}
        for decl in RUST_DECL.finditer(text):
            indent = len(decl["indent"])
            if decl["test"]:
                rust.add("::".join((*parts, *parents.values(), decl["test"])))
            elif decl["module"]:
                parents[indent] = decl["module"]
            else:
                parents.pop(indent, None)
    return java, rust


def step_selectors(text):
    arrays = dict(re.findall(r"^\s*(\w+)=\(\s*([^()]*)\)", text, re.M))
    directory = re.search(r"^\s*working-directory: (\S+)", text, re.M)
    directory = Path(directory[1]) if directory else Path()
    for line in text.replace("\\\n", " ").splitlines():
        if line.lstrip().startswith("#"):
            continue
        if "grep " in line:
            for name in re.findall(r"(?:\^test (?:\.\*::)?|::)(\w+(?:::\w+)*) (?=\\?\.)", line):
                yield "rust-test", name
        for name in re.findall(r'--source-class ([\w.]+)|"([\w.]+#\w+)/\d+"', line):
            yield "java", name[0] or name[1]
        for value in re.findall(r'''-Dtest=["']?(\$\{\w+\[\*\]\}|[\w#.,]+)''', line):
            if value.startswith("${"):
                names = shlex.split(arrays[value[2:-4]])
            else:
                names = value.split(",")
            for name in names:
                yield "java", name

        command = re.match(r"\s*(?:run:\s*)?(?:cargo test|(?:\.\./|\./)+build/[\w-]+-\*)\s+", line)
        if command:
            args = iter(shlex.split(line[command.end():].split("|", 1)[0]))
            for arg in args:
                if arg in ("-p", "--package", "--manifest-path", "--features", "--target",
                           "--test-threads", "--storage"):
                    next(args, None)
                elif re.fullmatch(r"\w+(?:::\w+)*(?:::)?", arg):
                    yield "rust-filter", arg

        discovery = re.search(r"unittest discover -s (\S+) -p ['\"]([^'\"]+)['\"]", line)
        if discovery:
            yield "python-glob", str(directory.joinpath(*discovery.groups()))
        if re.search(r"\bpytest\s", line):
            for name in re.findall(r"(?:^|\s)(tests/[\w./:-]+)", line):
                yield "pytest", str(directory / name)


def selectors(text):
    # A relative path and a shell array belong to the step that uses them.
    for step in re.split(r"^      - ", text, flags=re.M):
        yield from step_selectors(step)


def pytest_exists(name):
    path, *members = name.split("::")
    target = ROOT / path
    if target.is_dir():
        return not members and any(target.rglob("test_*.py"))
    if not target.is_file():
        return False
    body = ast.parse(target.read_text()).body
    for member in members:
        definition = next((node for node in body if getattr(node, "name", None) == member), None)
        if definition is None:
            return False
        body = definition.body
    return True


def main(workflows=None):
    java, rust = sources()
    missing = 0
    if workflows is None:
        workflows = sorted((ROOT / ".github/workflows").glob("*.y*ml"))
    for workflow in workflows:
        for kind, name in selectors(workflow.read_text()):
            if kind == "java":
                cls, _, method = name.partition("#")
                exists = cls in java and (not method or method in java[cls])
            elif kind == "rust-test":
                exists = name in rust or any(test.endswith("::" + name) for test in rust)
            elif kind == "rust-filter":
                exists = any(name in test for test in rust)
            elif kind == "pytest":
                exists = pytest_exists(name)
            else:
                exists = any(ROOT.glob(name))
            if not exists:
                print(f"{workflow.name}: no test matches {name}. Update the workflow selector.", file=sys.stderr)
                missing += 1
    return int(missing > 0)


if __name__ == "__main__":
    sys.exit(main(list(map(Path, sys.argv[1:])) or None))
