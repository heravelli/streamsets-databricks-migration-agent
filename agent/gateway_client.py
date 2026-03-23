"""
AI Gateway client — speaks the OpenAI-compatible chat completions API.

[LLM] This module makes real LLM calls through your enterprise AI gateway.
      Every call to messages_create() sends a request to the gateway endpoint and consumes tokens.
      The gateway routes to the model specified by AI_GATEWAY_MODEL.
      Configured via: AI_GATEWAY_URL, AI_GATEWAY_TOKEN, AI_GATEWAY_MODEL

      Token note: If your gateway enforces per-request token limits, set
      AGENT_COMPACT_CONTEXT=true in .env to reduce input prompt size ~60%.

Use when your organisation routes AI traffic through a central gateway
(LiteLLM, Azure AI Gateway, Apigee, etc.) instead of calling Anthropic directly.

Configure via .env:
    AGENT_CLIENT_TYPE=gateway
    AI_GATEWAY_URL=https://ai-gateway.yourcompany.com/v1
    AI_GATEWAY_TOKEN=<token-from-gateway-team>
    AI_GATEWAY_MODEL=<model-name-as-exposed-by-gateway>

The response is wrapped in an Anthropic-compatible interface so MigrationAgent
requires zero changes.
"""
from __future__ import annotations

import asyncio
import json
import uuid

import httpx

from config.settings import settings


# ── Response objects that mirror the Anthropic SDK interface ──────────────────

class _ToolUseBlock:
    type = "tool_use"

    def __init__(self, block_id: str, name: str, input_: dict):
        self.id = block_id
        self.name = name
        self.input = input_


class _TextBlock:
    type = "text"

    def __init__(self, text: str):
        self.text = text


class _GatewayResponse:
    """Wraps an OpenAI-compatible response in the Anthropic-compatible shape."""

    def __init__(self, stop_reason: str, content: list):
        self.stop_reason = stop_reason   # "tool_use" | "end_turn"
        self.content = content           # list of _ToolUseBlock | _TextBlock


# ── Format converters ─────────────────────────────────────────────────────────

def _anthropic_tools_to_openai(tools: list[dict]) -> list[dict]:
    """
    Anthropic tool schema → OpenAI tool schema.

    Anthropic:  {"name": ..., "description": ..., "input_schema": {...}}
    OpenAI:     {"type": "function", "function": {"name": ..., "description": ..., "parameters": {...}}}
    """
    result = []
    for tool in tools:
        result.append({
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema", {"type": "object", "properties": {}}),
            },
        })
    return result


def _anthropic_messages_to_openai(messages: list[dict], system: str) -> list[dict]:
    """
    Convert Anthropic-style message list + system string to OpenAI message list.

    Anthropic content blocks (tool_use / tool_result) → OpenAI tool_calls / tool role.
    """
    result: list[dict] = []

    if system:
        result.append({"role": "system", "content": system})

    for msg in messages:
        role = msg["role"]
        content = msg["content"]

        if isinstance(content, str):
            result.append({"role": role, "content": content})
            continue

        if not isinstance(content, list):
            result.append({"role": role, "content": str(content)})
            continue

        # --- assistant turn with tool_use blocks ---
        if role == "assistant":
            text_parts = []
            tool_calls = []
            for block in content:
                btype = getattr(block, "type", block.get("type") if isinstance(block, dict) else None)
                if btype == "tool_use":
                    name = getattr(block, "name", block.get("name"))
                    bid = getattr(block, "id", block.get("id", str(uuid.uuid4())))
                    inp = getattr(block, "input", block.get("input", {}))
                    tool_calls.append({
                        "id": bid,
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(inp)},
                    })
                elif btype == "text":
                    text = getattr(block, "text", block.get("text", ""))
                    if text:
                        text_parts.append(text)

            out: dict = {"role": "assistant"}
            if text_parts:
                out["content"] = " ".join(text_parts)
            if tool_calls:
                out["tool_calls"] = tool_calls
            result.append(out)

        # --- user turn with tool_result blocks ---
        elif role == "user":
            for block in content:
                btype = block.get("type") if isinstance(block, dict) else getattr(block, "type", None)
                if btype == "tool_result":
                    tool_use_id = block.get("tool_use_id") if isinstance(block, dict) else getattr(block, "tool_use_id", "")
                    tool_content = block.get("content") if isinstance(block, dict) else getattr(block, "content", "")
                    if isinstance(tool_content, list):
                        tool_content = json.dumps(tool_content)
                    result.append({
                        "role": "tool",
                        "tool_call_id": tool_use_id,
                        "content": str(tool_content),
                    })
                else:
                    text = block.get("text", "") if isinstance(block, dict) else getattr(block, "text", str(block))
                    if text:
                        result.append({"role": "user", "content": text})

    return result


def _openai_response_to_anthropic(raw: dict) -> _GatewayResponse:
    """Convert an OpenAI-compatible response dict to the Anthropic-compatible wrapper."""
    choice = raw["choices"][0]
    message = choice["message"]
    finish_reason = choice.get("finish_reason", "stop")

    content_blocks: list = []

    # Text content
    if message.get("content"):
        content_blocks.append(_TextBlock(message["content"]))

    # Tool calls
    tool_calls = message.get("tool_calls") or []
    for tc in tool_calls:
        fn = tc["function"]
        try:
            inp = json.loads(fn["arguments"])
        except (json.JSONDecodeError, TypeError):
            inp = {}
        content_blocks.append(_ToolUseBlock(tc["id"], fn["name"], inp))

    stop_reason = "tool_use" if tool_calls else "end_turn"
    return _GatewayResponse(stop_reason=stop_reason, content=content_blocks)


# ── Gateway client ────────────────────────────────────────────────────────────

class GatewayClient:
    """
    Calls an OpenAI-compatible AI gateway endpoint.
    Returns responses shaped like the Anthropic SDK so MigrationAgent is unchanged.
    """

    def __init__(self):
        if not settings.ai_gateway_url:
            raise ValueError("AI_GATEWAY_URL must be set when using gateway client")
        if not settings.ai_gateway_token:
            raise ValueError("AI_GATEWAY_TOKEN must be set when using gateway client")

        self._http = httpx.AsyncClient(
            base_url=settings.ai_gateway_url.rstrip("/"),
            headers={
                "Authorization": f"Bearer {settings.ai_gateway_token}",
                "Content-Type": "application/json",
            },
            timeout=120.0,
        )
        self.model = settings.ai_gateway_model

    async def messages_create(
        self,
        messages: list[dict],
        system: str,
        tools: list[dict],
        max_tokens: int | None = None,
    ) -> _GatewayResponse:
        max_tokens = max_tokens or settings.agent_max_tokens
        payload = {
            "model": self.model,
            "messages": _anthropic_messages_to_openai(messages, system),
            "tools": _anthropic_tools_to_openai(tools),
            "tool_choice": "auto",
            "max_tokens": max_tokens,
        }

        for attempt in range(3):
            try:
                # [LLM CALL] Sends request to AI gateway → forwards to AI_GATEWAY_MODEL
                resp = await self._http.post("/chat/completions", json=payload)
                if resp.status_code == 429:
                    if attempt == 2:
                        resp.raise_for_status()
                    await asyncio.sleep(2 ** attempt * 5)
                    continue
                resp.raise_for_status()
                return _openai_response_to_anthropic(resp.json())
            except httpx.HTTPStatusError as e:
                if e.response.status_code >= 500 and attempt < 2:
                    await asyncio.sleep(2 ** attempt * 2)
                else:
                    raise

        raise RuntimeError("Gateway request failed after 3 attempts")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self._http.aclose()
