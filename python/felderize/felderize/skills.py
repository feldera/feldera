from __future__ import annotations

import sys
from pathlib import Path

import yaml

from felderize.constants import FELDERIZE_DIR, PROMPTS_DIR, SKILLS_DIR
from felderize.docs import load_docs, load_examples


def load_skills(skills_dir: str | Path | None = None) -> str:
    """Load all skill .md files and return a combined prompt string.

    skills_dir defaults to the bundled skills/ directory; override in tests.
    """
    if skills_dir is None:
        skills_dir = SKILLS_DIR
    else:
        skills_dir = Path(skills_dir)

    if not skills_dir.is_dir():
        return ""

    sections: list[str] = []
    for md_file in sorted(skills_dir.glob("*.md")):
        raw = md_file.read_text()

        if not raw.startswith("---"):
            print(
                f"skills.py: skipping {md_file.name} — missing frontmatter",
                file=sys.stderr,
            )
            continue
        parts = raw.split("---", 2)
        if len(parts) < 3:
            print(
                f"skills.py: skipping {md_file.name} — malformed frontmatter",
                file=sys.stderr,
            )
            continue

        name = md_file.stem
        try:
            meta = yaml.safe_load(parts[1])
            if isinstance(meta, dict):
                name = meta.get("name", name)
        except yaml.YAMLError as e:
            print(
                f"skills.py: skipping {md_file.name} — invalid YAML frontmatter: {e}",
                file=sys.stderr,
            )
            continue

        body = parts[2].strip()
        sections.append(f"## Skill: {name}\n\n{body}")

    return "\n\n---\n\n".join(sections)


_prompt_header: str | None = None


def _get_prompt_header() -> str:
    """Load and cache the system prompt header from prompts/system_prompt.md."""
    global _prompt_header
    if _prompt_header is None:
        path = PROMPTS_DIR / "system_prompt.md"
        if not path.is_file():
            raise FileNotFoundError(
                f"System prompt not found: {path}. "
                "The felderize package may be incomplete."
            )
        _prompt_header = path.read_text()
    return _prompt_header


def _load_prompt(name: str) -> str:
    """Load a prompt template from the prompts/ directory."""
    return (PROMPTS_DIR / name).read_text()


def _auto_rule_dirs() -> list[Path]:
    """User-owned rule directories, searched automatically (lowest → highest priority)."""
    return [
        FELDERIZE_DIR / "rules",
        Path.cwd() / ".felderize" / "rules",
    ]


def _auto_example_dirs() -> list[Path]:
    """User-owned example directories, searched automatically (lowest → highest priority)."""
    return [
        FELDERIZE_DIR / "examples",
        Path.cwd() / ".felderize" / "examples",
    ]


def _scan_rules_dir(rules_dir: Path) -> list[str]:
    """Return non-empty rule texts from rules_dir/*.md, warning when frontmatter is absent."""
    result = []
    for f in sorted(rules_dir.glob("*.md")):
        text = f.read_text().strip()
        if not text:
            continue
        if not text.startswith("---"):
            print(
                f"skills.py: {f.name} has no frontmatter — consider adding name/description",
                file=sys.stderr,
            )
        result.append(text)
    return result


def _load_user_rules(explicit_files: list[Path] | None) -> str:
    """Load user rules from auto-discovered dirs and optional explicit files.

    Priority (additive, lowest → highest):
      1. ~/.felderize/rules/
      2. .felderize/rules/ in CWD
      3. Explicit --rules files (in order given)
    """
    parts: list[str] = []
    for d in _auto_rule_dirs():
        if d.is_dir():
            parts.extend(_scan_rules_dir(d))
    for f in explicit_files or []:
        if f.is_file():
            text = f.read_text().strip()
            if not text:
                continue
            if not text.startswith("---"):
                print(
                    f"skills.py: {f.name} has no frontmatter — consider adding name/description",
                    file=sys.stderr,
                )
            parts.append(text)
    return "\n\n---\n\n".join(parts)


def build_system_prompt(
    spark_sql: str = "",
    with_docs: bool = False,
    with_examples: bool = True,
    with_skills: bool = True,
    retry_unsupported: list[str] | None = None,
    extra_rules: list[Path] | None = None,
    extra_examples_dirs: list[Path] | None = None,
    extra_examples_files: list[Path] | None = None,
) -> str:
    """Build the full system prompt for a translation request.

    Assembles the prompt header, skills, user rules, examples, and (optionally)
    Feldera reference docs. Pass spark_sql so category detection can select
    relevant docs and examples; omit it to get a skills-only prompt.
    """
    skills_text = load_skills() if with_skills else ""
    prompt = _get_prompt_header()
    if retry_unsupported:
        items = "\n".join(f"- {u}" for u in retry_unsupported)
        preamble = _load_prompt("retry_preamble.md").format(unsupported_list=items)
        prompt += "\n\n" + preamble
    if skills_text:
        prompt += "\n\n" + skills_text
    rules_text = _load_user_rules(extra_rules)
    if rules_text:
        prompt += "\n\n## User Rules\n\n" + rules_text
    if spark_sql:
        if with_examples:
            auto_dirs: list[Path] = [d for d in _auto_example_dirs() if d.is_dir()]
            all_dirs = auto_dirs + (extra_examples_dirs or [])
            examples_text = load_examples(
                spark_sql,
                extra_examples_dirs=all_dirs,
                extra_examples_files=extra_examples_files,
            )
            if examples_text:
                prompt += (
                    "\n\n## Validated Translation Examples\n\n"
                    "These examples were validated against the Feldera compiler. "
                    "Follow the same patterns.\n\n" + examples_text
                )
        if with_docs:
            docs_text = load_docs(spark_sql)
            if docs_text:
                prompt += (
                    "\n\n## Feldera SQL Reference Documentation\n\n"
                    "NOTE: The translation rules above take precedence over the reference "
                    "documentation below. If there is any conflict, follow the rules above.\n\n"
                    + docs_text
                )
    return prompt
