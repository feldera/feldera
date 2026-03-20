from __future__ import annotations

from abc import ABC, abstractmethod

from felderize.config import Config


class LLMClient(ABC):
    @abstractmethod
    def translate(self, system_prompt: str, user_prompt: str) -> str:
        """Send a translation request and return the raw response text."""


class AnthropicClient(LLMClient):
    def __init__(self, config: Config):
        import anthropic

        self.client = anthropic.Anthropic(api_key=config.api_key)
        self.model = config.model

    def translate(self, system_prompt: str, user_prompt: str) -> str:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        return response.content[0].text


def create_client(config: Config) -> LLMClient:
    return AnthropicClient(config)
