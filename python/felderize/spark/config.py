from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class Config:
    llm_provider: str = "anthropic"
    model: str = ""
    api_key: str = ""
    feldera_compiler: str = ""

    @classmethod
    def from_env(cls) -> Config:
        # Load .env from project root (won't override existing env vars)
        env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(env_path)

        provider = os.environ.get("FELDERIZE_LLM_PROVIDER", "anthropic")

        if provider == "openai":
            default_model = "gpt-4o"
            api_key = os.environ.get("OPENAI_API_KEY", "")
        else:
            default_model = "claude-sonnet-4-20250514"
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")

        return cls(
            llm_provider=provider,
            model=os.environ.get("FELDERIZE_MODEL", default_model),
            api_key=api_key,
            feldera_compiler=os.environ.get("FELDERA_COMPILER", ""),
        )
