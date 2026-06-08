from __future__ import annotations

from pathlib import Path

# Base directory for all felderize user data (compiler JAR, rules, examples).
FELDERIZE_DIR = Path.home() / ".felderize"

# GitHub releases API for the Feldera repo.
GITHUB_RELEASES_API = "https://api.github.com/repos/feldera/feldera/releases"

# Prefix of the compiler JAR asset in GitHub releases.
COMPILER_JAR_PREFIX = "sql2dbsp-jar-with-dependencies"

# Minimum Feldera compiler release felderize supports. Older releases lack
# features felderize relies on (e.g. div_null, MAKE_DATE), so translations
# validated against them may be inaccurate.
MINIMUM_COMPILER_VERSION = "v0.304.0"

# Timeout in seconds for outbound HTTP requests (GitHub API, docs fetch).
HTTP_TIMEOUT = 30

# Default Feldera docs base URL (raw GitHub, main branch).
DEFAULT_DOCS_BASE_URL = (
    "https://raw.githubusercontent.com/feldera/feldera/main/docs.feldera.com/docs/sql"
)

# Default LLM max tokens.
DEFAULT_MAX_TOKENS = 64000

# Default number of validation+repair attempts before giving up.
DEFAULT_MAX_RETRIES = 3

# Timeout in seconds for the Feldera compiler subprocess.
COMPILER_TIMEOUT = 600

# Bundled prompt templates directory.
PROMPTS_DIR = Path(__file__).resolve().parent / "prompts"

# Bundled skills directory.
SKILLS_DIR = Path(__file__).resolve().parent / "skills"
