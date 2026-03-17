"""
Abstract protocol for AI clients.

Both ClaudeClient (direct Anthropic SDK) and GatewayClient (OpenAI-compatible
enterprise gateway) implement this interface so MigrationAgent stays unchanged.
"""
from typing import Protocol, runtime_checkable


@runtime_checkable
class BaseAgentClient(Protocol):
    async def messages_create(
        self,
        messages: list[dict],
        system: str,
        tools: list[dict],
        max_tokens: int | None = None,
    ) -> object:
        """
        Send messages and return a response object that exposes:
          .stop_reason  — "tool_use" | "end_turn"
          .content      — list of blocks; tool-use blocks have .type, .id, .name, .input
        """
        ...
