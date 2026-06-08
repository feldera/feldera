from __future__ import annotations

import sys
import time
from abc import ABC, abstractmethod

import anthropic
import httpx

from felderize.config import Config

_RATE_LIMIT_RETRIES = 5


class PromptTooLargeError(Exception):
    """The request exceeded the model's context window.

    Raised instead of the raw ``anthropic.BadRequestError`` so callers can turn a
    too-large program into a clear "shorten the input" message rather than an
    opaque API error. Retrying does not help, so this is never retried.
    """


def _is_context_length_error(error: anthropic.BadRequestError) -> bool:
    """Tell a context-window overflow apart from other 400 errors.

    Anthropic reports an oversized request with a message such as
    ``prompt is too long: N tokens > M maximum``. Match that phrasing (and a few
    near-synonyms) so genuine bad requests still surface unchanged.
    """
    message = str(error).lower()
    if (
        "too long" in message
        or "context window" in message
        or "context length" in message
    ):
        return True
    return "maximum" in message and "token" in message


# Transient failures worth retrying: dropped/again-refused connections, timeouts,
# and 5xx. The raw httpx errors are included because a streamed response can drop
# mid-body (RemoteProtocolError) without the SDK wrapping it.
_TRANSIENT_ERRORS = (
    anthropic.APIConnectionError,
    anthropic.APITimeoutError,
    anthropic.InternalServerError,
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.ConnectError,
)


class LLMClient(ABC):
    """Abstract interface for LLM translation backends."""

    @abstractmethod
    def translate(self, system_prompt: str, user_prompt: str) -> str:
        """Send a translation request and return the raw response text."""


class AnthropicClient(LLMClient):
    """LLM client backed by the Anthropic API."""

    def __init__(self, config: Config, verbose: bool = False):
        self.client = anthropic.Anthropic(
            api_key=config.api_key, base_url=config.base_url
        )
        self.model = config.model
        self.max_tokens = config.max_tokens
        self.verbose = verbose

    def translate(self, system_prompt: str, user_prompt: str) -> str:
        """Send a prompt to the Anthropic API and return the response text.

        Retries up to _RATE_LIMIT_RETRIES times on rate limit errors, with
        linearly increasing back-off.
        """
        for attempt in range(_RATE_LIMIT_RETRIES):
            try:
                with self.client.messages.stream(
                    model=self.model,
                    max_tokens=self.max_tokens,
                    system=[
                        {
                            "type": "text",
                            "text": system_prompt,
                            "cache_control": {"type": "ephemeral"},
                        }
                    ],
                    messages=[{"role": "user", "content": user_prompt}],
                ) as stream:
                    response = stream.get_final_message()
                u = response.usage
                if self.verbose:
                    print(
                        f"    llm: input={u.input_tokens} "
                        f"cache_read={getattr(u, 'cache_read_input_tokens', 0)} "
                        f"cache_write={getattr(u, 'cache_creation_input_tokens', 0)} "
                        f"output={u.output_tokens}",
                        file=sys.stderr,
                    )
                return response.content[0].text
            except anthropic.BadRequestError as e:
                if _is_context_length_error(e):
                    raise PromptTooLargeError(str(e)) from e
                raise
            except anthropic.RateLimitError:
                if attempt == _RATE_LIMIT_RETRIES - 1:
                    raise
                wait = 60 * (attempt + 1)
                print(f"Rate limited — waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
            except _TRANSIENT_ERRORS as e:
                if attempt == _RATE_LIMIT_RETRIES - 1:
                    raise
                wait = 5 * (attempt + 1)
                print(
                    f"Transient API error ({type(e).__name__}) — retrying in {wait}s...",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(wait)
        raise AssertionError("unreachable")


def create_client(config: Config, verbose: bool = False) -> LLMClient:
    """Instantiate the appropriate LLM client for the given config."""
    return AnthropicClient(config, verbose=verbose)
