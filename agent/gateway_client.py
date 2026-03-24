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

    # Optional — override if your gateway puts the model in the URL path:
    AI_GATEWAY_COMPLETIONS_PATH=/chat/{model}
    # With the above setting the client POSTs to:
    #   https://ai-gateway.yourcompany.com/chat/claude-sonnet-4-6
    # instead of the default:
    #   https://ai-gateway.yourcompany.com/v1/chat/completions

The response is wrapped in an Anthropic-compatible interface so MigrationAgent
requires zero changes.
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid

import httpx

from config.settings import settings

log = logging.getLogger(__name__)


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
                # Use isinstance guard before .get() — Python evaluates all
                # function arguments eagerly, so getattr(obj, k, obj.get(k))
                # would call obj.get(k) even when obj has the attribute.
                if isinstance(block, dict):
                    btype = block.get("type")
                    name  = block.get("name")
                    bid   = block.get("id", str(uuid.uuid4()))
                    inp   = block.get("input", {})
                    text  = block.get("text", "")
                else:
                    btype = getattr(block, "type", None)
                    name  = getattr(block, "name", None)
                    bid   = getattr(block, "id", str(uuid.uuid4()))
                    inp   = getattr(block, "input", {})
                    text  = getattr(block, "text", "")

                if btype == "tool_use":
                    tool_calls.append({
                        "id": bid,
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(inp)},
                    })
                elif btype == "text":
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
    Calls any OpenAI-compatible endpoint (enterprise gateway or OpenAI directly).
    Returns responses shaped like the Anthropic SDK so MigrationAgent is unchanged.

    url, token, model can be passed explicitly (e.g. for OpenAI) or left as None
    to fall back to AI_GATEWAY_URL / AI_GATEWAY_TOKEN / AI_GATEWAY_MODEL settings.
    """

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        model: str | None = None,
        completions_path: str | None = None,
    ):
        resolved_url = url or settings.ai_gateway_url
        resolved_token = token or settings.ai_gateway_token
        resolved_model = model or settings.ai_gateway_model
        resolved_path = completions_path or settings.ai_gateway_completions_path

        if not resolved_url:
            raise ValueError("AI_GATEWAY_URL must be set when using gateway client")
        if not resolved_token:
            raise ValueError("AI_GATEWAY_TOKEN must be set when using gateway client")

        # If the completions path contains "{model}" the model is embedded in the URL.
        # In that case we POST to the resolved full URL and omit "model" from the body
        # (some gateways reject unknown body fields).
        self._model_in_path = "{model}" in resolved_path
        # Substitute model into path now so we don't repeat it on every request.
        self._completions_path = resolved_path.format(model=resolved_model) if self._model_in_path else resolved_path

        self._full_url = resolved_url.rstrip("/") + self._completions_path
        log.info("GatewayClient initialised — will POST to: %s", self._full_url)

        # Build auth header.
        # Most gateways: "Authorization: Bearer <token>"
        # Some enterprise gateways (e.g. Apigee / Azure AI Foundry):
        #   "api-key: <token>"  ← set AI_GATEWAY_AUTH_HEADER=api-key
        auth_header_name = settings.ai_gateway_auth_header
        if auth_header_name.lower() == "authorization":
            auth_value = f"Bearer {resolved_token}"
        else:
            auth_value = resolved_token  # e.g. api-key: <raw token, no "Bearer">

        headers: dict[str, str] = {
            auth_header_name: auth_value,
            "Content-Type": "application/json",
        }

        # Merge any extra headers from AI_GATEWAY_EXTRA_HEADERS
        # Format: "ai-gateway-version:v2,x-custom:foo"
        if settings.ai_gateway_extra_headers:
            for pair in settings.ai_gateway_extra_headers.split(","):
                pair = pair.strip()
                if ":" in pair:
                    k, v = pair.split(":", 1)
                    headers[k.strip()] = v.strip()

        log.debug("Gateway request headers (redacted): %s", {k: ("***" if "key" in k.lower() or "auth" in k.lower() else v) for k, v in headers.items()})

        self._http = httpx.AsyncClient(
            headers=headers,
            timeout=120.0,
            # Follow 301/302 redirects automatically.
            # Enterprise gateways often redirect for trailing-slash mismatches
            # or HTTP→HTTPS upgrades. httpx does NOT follow redirects by default.
            follow_redirects=True,
        )
        self.model = resolved_model

    async def messages_create(
        self,
        messages: list[dict],
        system: str,
        tools: list[dict],
        max_tokens: int | None = None,
    ) -> _GatewayResponse:
        max_tokens = max_tokens or settings.agent_max_tokens
        payload: dict = {
            "messages": _anthropic_messages_to_openai(messages, system),
            "tools": _anthropic_tools_to_openai(tools),
            # "auto" string is valid OpenAI format; some strict gateways want the
            # object form {"type": "auto"} — both are sent as "auto" here which
            # all major Azure AI / LiteLLM / Apigee gateways accept.
            "tool_choice": "auto",
            "max_tokens": max_tokens,
            # Explicitly disable streaming — enterprise gateways sometimes default
            # to stream=true which returns SSE chunks our parser cannot handle.
            "stream": False,
        }
        # Omit "model" from the body when it is already encoded in the URL path
        # (some gateways reject unknown body fields; others route by URL not body).
        if not self._model_in_path:
            payload["model"] = self.model

        for attempt in range(3):
            try:
                # [LLM CALL] Sends request to AI gateway → forwards to AI_GATEWAY_MODEL
                log.debug("Gateway POST attempt %d → %s", attempt + 1, self._full_url)
                resp = await self._http.post(self._full_url, json=payload)
                if resp.history:
                    # Log redirect chain so misconfigured paths are easy to diagnose
                    for r in resp.history:
                        log.warning("Gateway redirect: %s %s → %s", r.status_code, r.url, r.headers.get("location"))
                # 429 = rate limited, 529 = gateway/model overloaded — both are retryable
                if resp.status_code in (429, 529):
                    if attempt == 2:
                        raise RuntimeError(
                            f"Gateway returned HTTP {resp.status_code} after 3 attempts "
                            f"(model overloaded). Try --concurrency 1 or retry later."
                        )
                    delay = 2 ** attempt * 5  # 5s, 10s, 20s
                    log.warning(
                        "Gateway returned %d (overloaded), retrying in %ds (attempt %d/3)",
                        resp.status_code, delay, attempt + 1,
                    )
                    await asyncio.sleep(delay)
                    continue
                if not resp.is_success:
                    # Surface the gateway error body so it is visible in logs
                    # (gateway teams often put the real error message in the JSON body,
                    #  not just the HTTP status line)
                    try:
                        err_body = resp.json()
                    except Exception:
                        err_body = resp.text
                    raise RuntimeError(
                        f"Gateway returned HTTP {resp.status_code} "
                        f"for POST {self._completions_path}: {err_body}"
                    )
                return _openai_response_to_anthropic(resp.json())
            except RuntimeError:
                raise
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
