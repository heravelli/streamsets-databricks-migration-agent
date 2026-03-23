"""
Direct Anthropic API client.

[LLM] This module makes real LLM calls to Anthropic's API.
      Every call to messages_create() sends a request to Anthropic and consumes tokens.
      Configured via: ANTHROPIC_API_KEY, ANTHROPIC_MODEL (default: claude-sonnet-4-6)
"""

import asyncio
import anthropic
from config.settings import settings


class ClaudeClient:
    def __init__(self):
        self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
        self.model = settings.anthropic_model

    async def messages_create(
        self,
        messages: list[dict],
        system: str,
        tools: list[dict],
        max_tokens: int | None = None,
    ) -> anthropic.types.Message:
        # [LLM CALL] Sends request to Anthropic API — consumes input + output tokens
        max_tokens = max_tokens or settings.agent_max_tokens
        for attempt in range(3):
            try:
                return await self._client.messages.create(
                    model=self.model,
                    max_tokens=max_tokens,
                    system=system,
                    tools=tools,
                    messages=messages,
                )
            except anthropic.RateLimitError:
                if attempt == 2:
                    raise
                await asyncio.sleep(2 ** attempt * 5)
            except anthropic.APIStatusError as e:
                if e.status_code >= 500 and attempt < 2:
                    await asyncio.sleep(2 ** attempt * 2)
                else:
                    raise
