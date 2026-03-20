from __future__ import annotations

from pathlib import Path

import yaml

from felderize.docs import load_docs, load_examples


def load_skills(skills_dir: str | Path | None = None) -> str:
    """Load all SKILL.md files and return a combined prompt string."""
    if skills_dir is None:
        skills_dir = Path(__file__).resolve().parent / "data" / "skills"
    else:
        skills_dir = Path(skills_dir)

    if not skills_dir.is_dir():
        return ""

    sections: list[str] = []
    md_files: list[Path] = sorted(skills_dir.glob("*.md"))
    for skill_path in sorted(skills_dir.iterdir()):
        if skill_path.is_dir():
            sub = skill_path / "SKILL.md"
            if sub.is_file():
                md_files.append(sub)

    for md_file in sorted(md_files):
        raw = md_file.read_text()

        # Parse YAML frontmatter
        name = md_file.stem
        body = raw
        if raw.startswith("---"):
            parts = raw.split("---", 2)
            if len(parts) >= 3:
                try:
                    meta = yaml.safe_load(parts[1])
                    if isinstance(meta, dict):
                        name = meta.get("name", name)
                except yaml.YAMLError:
                    pass
                body = parts[2].strip()

        sections.append(f"## Skill: {name}\n\n{body}")

    return "\n\n---\n\n".join(sections)


SYSTEM_PROMPT_PREFIX = """\
You are a Spark SQL to Feldera SQL translator. Your job is to convert Spark SQL \
schemas and queries into valid Feldera SQL.

Apply the translation rules below strictly. If a Spark construct has no Feldera \
equivalent, list it under "unsupported" rather than guessing.

Respond ONLY with a JSON object (no markdown fences) with these keys:
- "feldera_schema": the translated CREATE TABLE / CREATE VIEW DDL statements
- "feldera_query": the translated query as one or more CREATE VIEW statements
- "unsupported": list of Spark constructs that cannot be translated
- "warnings": list of translation notes or approximations made
- "explanations": list of transformations applied (e.g., "Rewrote LPAD(...) as CONCAT(REPEAT(...), ...)")

Translation rules:
"""


def build_system_prompt(
    skills_dir: str | Path | None = None,
    docs_dir: Path | None = None,
    spark_sql: str = "",
    with_docs: bool = False,
    with_examples: bool = True,
    with_skills: bool = True,
) -> str:
    prompt = SYSTEM_PROMPT_PREFIX
    if with_skills:
        skills_text = load_skills(skills_dir)
        prompt += "\n\n" + skills_text
    if spark_sql:
        if with_examples:
            examples_text = load_examples(spark_sql)
            if examples_text:
                prompt += (
                    "\n\n## Validated Translation Examples\n\n"
                    "These examples were validated against the Feldera compiler. "
                    "Follow the same patterns.\n\n" + examples_text
                )
        if with_docs:
            docs_text = load_docs(spark_sql, docs_dir)
            if docs_text:
                prompt += (
                    "\n\n## Feldera SQL Reference Documentation\n\n"
                    "NOTE: The translation rules above take precedence over the reference "
                    "documentation below. If there is any conflict, follow the rules above.\n\n"
                    + docs_text
                )
    return prompt
