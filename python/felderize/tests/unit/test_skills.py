"""Unit tests for skill loading, user-rule discovery, and prompt assembly."""

from __future__ import annotations

from felderize import skills


# ---------------------------------------------------------------------------
# load_skills — frontmatter handling
# ---------------------------------------------------------------------------


def _write(dir_path, name, text):
    f = dir_path / name
    f.write_text(text)
    return f


def test_load_skills_uses_frontmatter_name(tmp_path):
    _write(tmp_path, "a.md", "---\nname: my-rules\n---\n\n- do a thing")
    out = skills.load_skills(tmp_path)
    assert "## Skill: my-rules" in out
    assert "- do a thing" in out


def test_load_skills_falls_back_to_stem_when_no_name(tmp_path):
    _write(tmp_path, "fallback.md", "---\ndescription: x\n---\n\nbody")
    out = skills.load_skills(tmp_path)
    assert "## Skill: fallback" in out


def test_load_skills_skips_missing_frontmatter(tmp_path, capsys):
    _write(tmp_path, "bad.md", "no frontmatter here")
    out = skills.load_skills(tmp_path)
    assert out == ""
    assert "missing frontmatter" in capsys.readouterr().err


def test_load_skills_skips_malformed_frontmatter(tmp_path, capsys):
    _write(tmp_path, "bad.md", "---\nonly one fence")
    out = skills.load_skills(tmp_path)
    assert out == ""
    assert "malformed frontmatter" in capsys.readouterr().err


def test_load_skills_skips_invalid_yaml(tmp_path, capsys):
    _write(tmp_path, "bad.md", "---\nname: [unclosed\n---\nbody")
    out = skills.load_skills(tmp_path)
    assert out == ""
    assert "invalid YAML" in capsys.readouterr().err


def test_load_skills_missing_dir_returns_empty(tmp_path):
    assert skills.load_skills(tmp_path / "nope") == ""


def test_load_skills_joins_multiple_files_in_order(tmp_path):
    _write(tmp_path, "1.md", "---\nname: one\n---\nfirst")
    _write(tmp_path, "2.md", "---\nname: two\n---\nsecond")
    out = skills.load_skills(tmp_path)
    assert out.index("one") < out.index("two")
    assert "\n\n---\n\n" in out  # section separator


# ---------------------------------------------------------------------------
# _scan_rules_dir / _load_user_rules
# ---------------------------------------------------------------------------


def test_scan_rules_dir_skips_empty_and_warns_on_no_frontmatter(tmp_path, capsys):
    _write(tmp_path, "empty.md", "   \n")
    _write(tmp_path, "plain.md", "- a project rule")
    _write(tmp_path, "good.md", "---\nname: r\n---\n- another rule")
    out = skills._scan_rules_dir(tmp_path)
    joined = "\n".join(out)
    assert "- a project rule" in joined
    assert "- another rule" in joined
    assert len(out) == 2  # empty file dropped
    assert "no frontmatter" in capsys.readouterr().err


def test_load_user_rules_additive_dirs_then_explicit(tmp_path, monkeypatch):
    auto = tmp_path / "auto"
    auto.mkdir()
    _write(auto, "auto.md", "---\nname: a\n---\nAUTO RULE")
    explicit = _write(tmp_path, "explicit.md", "---\nname: e\n---\nEXPLICIT RULE")
    monkeypatch.setattr(skills, "_auto_rule_dirs", lambda: [auto])
    out = skills._load_user_rules([explicit])
    assert "AUTO RULE" in out and "EXPLICIT RULE" in out
    assert out.index("AUTO RULE") < out.index("EXPLICIT RULE")


def test_load_user_rules_explicit_empty_file_skipped(tmp_path, monkeypatch):
    monkeypatch.setattr(skills, "_auto_rule_dirs", lambda: [])
    empty = _write(tmp_path, "empty.md", "")
    out = skills._load_user_rules([empty])
    assert out == ""


# ---------------------------------------------------------------------------
# build_system_prompt — assembly under flag combinations
# ---------------------------------------------------------------------------


def _isolate(monkeypatch):
    """Stop build_system_prompt from picking up the developer's ~/.felderize dirs."""
    monkeypatch.setattr(skills, "_auto_rule_dirs", lambda: [])
    monkeypatch.setattr(skills, "_auto_example_dirs", lambda: [])


def test_build_prompt_includes_skills_by_default(monkeypatch):
    _isolate(monkeypatch)
    prompt = skills.build_system_prompt()
    assert "## Skill:" in prompt  # bundled skills loaded


def test_build_prompt_without_skills(monkeypatch):
    _isolate(monkeypatch)
    prompt = skills.build_system_prompt(with_skills=False)
    assert "## Skill:" not in prompt


def test_build_prompt_inserts_retry_preamble(monkeypatch):
    _isolate(monkeypatch)
    prompt = skills.build_system_prompt(retry_unsupported=["reverse(s)"])
    assert "reverse(s)" in prompt


def test_build_prompt_appends_user_rules(tmp_path, monkeypatch):
    _isolate(monkeypatch)
    rule = _write(tmp_path, "r.md", "---\nname: proj\n---\nPROJECT SPECIFIC RULE")
    prompt = skills.build_system_prompt(extra_rules=[rule])
    assert "## User Rules" in prompt
    assert "PROJECT SPECIFIC RULE" in prompt


def test_build_prompt_appends_matching_example(tmp_path, monkeypatch):
    _isolate(monkeypatch)
    ex = _write(
        tmp_path, "ex.md", "---\ncategories: [string]\n---\nUPPERCASE EXAMPLE BODY"
    )
    prompt = skills.build_system_prompt(
        spark_sql="SELECT UPPER(name) FROM t",
        extra_examples_files=[ex],
    )
    assert "Validated Translation Examples" in prompt
    assert "UPPERCASE EXAMPLE BODY" in prompt


def test_build_prompt_skills_only_when_no_sql(tmp_path, monkeypatch):
    _isolate(monkeypatch)
    ex = _write(tmp_path, "ex.md", "---\ncategories: [string]\n---\nEXAMPLE BODY")
    # spark_sql defaults to "" → examples are never consulted.
    prompt = skills.build_system_prompt(extra_examples_files=[ex])
    assert "Validated Translation Examples" not in prompt
