from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from felderize.constants import DEFAULT_DOCS_BASE_URL, DEFAULT_MAX_TOKENS


@dataclass
class Config:
    """Runtime configuration for felderize, populated from environment variables."""

    model: str = ""
    api_key: str | None = None
    base_url: str | None = None
    feldera_compiler: str = ""
    max_tokens: int = DEFAULT_MAX_TOKENS
    docs_base_url: str = DEFAULT_DOCS_BASE_URL

    @property
    def compiler_path(self) -> str | None:
        """Return the compiler path, or None if not set."""
        return self.feldera_compiler or None

    @classmethod
    def from_env(cls) -> Config:
        # Load .env from project root (won't override existing env vars)
        env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(env_path)

        raw_max_tokens = os.environ.get("FELDERIZE_MAX_TOKENS")
        return cls(
            model=os.environ.get("FELDERIZE_MODEL", ""),
            api_key=os.environ.get("ANTHROPIC_API_KEY"),
            base_url=os.environ.get("ANTHROPIC_BASE_URL"),
            feldera_compiler=os.environ.get("FELDERA_COMPILER", ""),
            max_tokens=int(raw_max_tokens) if raw_max_tokens else DEFAULT_MAX_TOKENS,
            docs_base_url=os.environ.get(
                "FELDERA_DOCS_BASE_URL", DEFAULT_DOCS_BASE_URL
            ),
        )
