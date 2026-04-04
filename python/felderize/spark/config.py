from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass
class Config:
    model: str = ""
    api_key: str | None = None
    base_url: str | None = None
    feldera_compiler: str = ""

    @classmethod
    def from_env(cls) -> Config:
        # Load .env from project root (won't override existing env vars)
        env_path = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(env_path)

        return cls(
            model=os.environ.get("FELDERIZE_MODEL", ""),
            api_key=os.environ.get("ANTHROPIC_API_KEY"),
            base_url=os.environ.get("ANTHROPIC_BASE_URL"),
            feldera_compiler=os.environ.get("FELDERA_COMPILER", ""),
        )
