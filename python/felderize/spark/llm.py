from __future__ import annotations

import sys
import time
from abc import ABC, abstractmethod

import anthropic

from felderize.config import Config


class LLMClient(ABC):
    @abstractmethod
    def translate(self, system_prompt: str, user_prompt: str) -> str:
        """Send a translation request and return the raw response text."""


class AnthropicClient(LLMClient):
    def __init__(self, config: Config):
        self.client = anthropic.Anthropic(api_key=config.api_key)
        self.model = config.model

    def translate(self, system_prompt: str, user_prompt: str) -> str:
        for attempt in range(5):
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=[{
                        "type": "text",
                        "text": system_prompt,
                        "cache_control": {"type": "ephemeral"},
                    }],
                    messages=[{"role": "user", "content": user_prompt}],
                )
                u = response.usage
                print(
                    f"    llm: input={u.input_tokens} "
                    f"cache_read={getattr(u, 'cache_read_input_tokens', 0)} "
                    f"cache_write={getattr(u, 'cache_creation_input_tokens', 0)} "
                    f"output={u.output_tokens}",
                    file=sys.stderr,
                )
                return response.content[0].text
            except anthropic.RateLimitError:
                if attempt == 4:
                    raise
                wait = 60 * (attempt + 1)
                print(f"Rate limited — waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
        raise AssertionError("unreachable")


def create_client(config: Config) -> LLMClient:
    return AnthropicClient(config)
