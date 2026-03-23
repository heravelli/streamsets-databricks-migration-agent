"""
Unit tests for agent/gateway_client.py

Covers:
  - Schema conversion helpers (_anthropic_tools_to_openai, _anthropic_messages_to_openai,
    _openai_response_to_anthropic)
  - _ToolUseBlock attribute access (regression test for the getattr/dict.get() bug)
  - GatewayClient.messages_create (HTTP mocked via pytest-mock)
"""
import json
import pytest

from agent.gateway_client import (
    _anthropic_tools_to_openai,
    _anthropic_messages_to_openai,
    _openai_response_to_anthropic,
    _ToolUseBlock,
    _TextBlock,
    _GatewayResponse,
    GatewayClient,
)


# ── _ToolUseBlock regression test ─────────────────────────────────────────────

class TestToolUseBlockAttributes:
    """
    Regression test for: '_ToolUseBlock' object has no attribute 'get'

    Root cause: Python evaluates ALL arguments before calling a function.
    `getattr(block, "name", block.get("name"))` would call `block.get()` even
    when `block` is a _ToolUseBlock (not a dict) — crashing because _ToolUseBlock
    has no `.get()` method.

    Fix: use `isinstance(block, dict)` guards before any `.get()` call.
    """

    def test_tool_use_block_has_type(self):
        block = _ToolUseBlock("id-1", "lookup_stage_mapping", {"stage_name": "foo"})
        assert block.type == "tool_use"

    def test_tool_use_block_has_name(self):
        block = _ToolUseBlock("id-1", "lookup_stage_mapping", {"stage_name": "foo"})
        assert block.name == "lookup_stage_mapping"

    def test_tool_use_block_has_id(self):
        block = _ToolUseBlock("id-1", "lookup_stage_mapping", {})
        assert block.id == "id-1"

    def test_tool_use_block_has_input(self):
        inp = {"stage_name": "com.streamsets.KafkaDSource"}
        block = _ToolUseBlock("id-1", "lookup_stage_mapping", inp)
        assert block.input == inp

    def test_tool_use_block_has_no_get_method(self):
        """Confirm _ToolUseBlock is NOT a dict — it must not have .get()."""
        block = _ToolUseBlock("id-1", "test_tool", {})
        assert not hasattr(block, "get"), "_ToolUseBlock must not have .get() — it is not a dict"

    def test_anthropic_messages_to_openai_with_tool_use_block_objects(self):
        """
        Critical regression: passing real _ToolUseBlock objects (not dicts) in an
        assistant message must not raise AttributeError.
        """
        block = _ToolUseBlock("call-1", "lookup_stage_mapping", {"stage_name": "foo"})
        messages = [
            {"role": "assistant", "content": [block]},
        ]
        # This would crash with the original bug: block.get("name") on a _ToolUseBlock
        result = _anthropic_messages_to_openai(messages, system="")
        assert any(m.get("tool_calls") for m in result)

    def test_anthropic_messages_to_openai_with_text_block_objects(self):
        """_TextBlock objects in assistant messages must be handled without .get()."""
        block = _TextBlock("Analysing the pipeline...")
        messages = [{"role": "assistant", "content": [block]}]
        result = _anthropic_messages_to_openai(messages, system="")
        assistant_msg = next(m for m in result if m["role"] == "assistant")
        assert "Analysing" in assistant_msg.get("content", "")


# ── _anthropic_tools_to_openai ────────────────────────────────────────────────

class TestAnthropic_to_OpenAIToolConversion:
    def test_basic_conversion(self):
        tools = [
            {
                "name": "classify_pipeline",
                "description": "Classify the pipeline",
                "input_schema": {
                    "type": "object",
                    "properties": {"has_streaming_origin": {"type": "boolean"}},
                    "required": ["has_streaming_origin"],
                },
            }
        ]
        result = _anthropic_tools_to_openai(tools)
        assert len(result) == 1
        assert result[0]["type"] == "function"
        assert result[0]["function"]["name"] == "classify_pipeline"
        assert result[0]["function"]["description"] == "Classify the pipeline"
        assert "properties" in result[0]["function"]["parameters"]

    def test_missing_description_defaults_empty(self):
        tools = [{"name": "my_tool", "input_schema": {"type": "object", "properties": {}}}]
        result = _anthropic_tools_to_openai(tools)
        assert result[0]["function"]["description"] == ""

    def test_missing_input_schema_defaults_empty_object(self):
        tools = [{"name": "my_tool"}]
        result = _anthropic_tools_to_openai(tools)
        assert result[0]["function"]["parameters"] == {"type": "object", "properties": {}}

    def test_multiple_tools(self):
        tools = [{"name": "tool_a"}, {"name": "tool_b"}]
        result = _anthropic_tools_to_openai(tools)
        assert len(result) == 2


# ── _anthropic_messages_to_openai ─────────────────────────────────────────────

class TestAnthropic_to_OpenAIMessageConversion:
    def test_system_injected_first(self):
        messages = [{"role": "user", "content": "hello"}]
        result = _anthropic_messages_to_openai(messages, system="You are an agent")
        assert result[0] == {"role": "system", "content": "You are an agent"}

    def test_no_system_if_empty(self):
        messages = [{"role": "user", "content": "hello"}]
        result = _anthropic_messages_to_openai(messages, system="")
        assert not any(m["role"] == "system" for m in result)

    def test_plain_string_user_message(self):
        messages = [{"role": "user", "content": "hello"}]
        result = _anthropic_messages_to_openai(messages, system="")
        assert result[0] == {"role": "user", "content": "hello"}

    def test_assistant_tool_call_dict_blocks(self):
        """Test with dict-style content blocks (as returned by Anthropic SDK serialization)."""
        messages = [
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "tool_use",
                        "id": "call-1",
                        "name": "lookup_stage_mapping",
                        "input": {"stage_name": "foo"},
                    }
                ],
            }
        ]
        result = _anthropic_messages_to_openai(messages, system="")
        asst = result[0]
        assert asst["role"] == "assistant"
        assert len(asst["tool_calls"]) == 1
        assert asst["tool_calls"][0]["function"]["name"] == "lookup_stage_mapping"

    def test_user_tool_result_dict_block(self):
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "tool_result",
                        "tool_use_id": "call-1",
                        "content": json.dumps({"found": True}),
                    }
                ],
            }
        ]
        result = _anthropic_messages_to_openai(messages, system="")
        tool_msg = result[0]
        assert tool_msg["role"] == "tool"
        assert tool_msg["tool_call_id"] == "call-1"

    def test_mixed_text_and_tool_calls_in_assistant(self):
        messages = [
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "I'll look this up."},
                    {"type": "tool_use", "id": "call-1", "name": "lookup_stage_mapping", "input": {}},
                ],
            }
        ]
        result = _anthropic_messages_to_openai(messages, system="")
        asst = result[0]
        assert "I'll look this up" in asst.get("content", "")
        assert len(asst["tool_calls"]) == 1


# ── _openai_response_to_anthropic ─────────────────────────────────────────────

class TestOpenAI_to_AnthropicResponseConversion:
    def _make_openai_response(self, content=None, tool_calls=None, finish_reason="stop"):
        message = {}
        if content:
            message["content"] = content
        if tool_calls:
            message["tool_calls"] = tool_calls
        return {
            "choices": [{"message": message, "finish_reason": finish_reason}]
        }

    def test_text_only_response(self):
        raw = self._make_openai_response(content="Hello from LLM")
        resp = _openai_response_to_anthropic(raw)
        assert resp.stop_reason == "end_turn"
        assert len(resp.content) == 1
        assert resp.content[0].text == "Hello from LLM"

    def test_tool_call_response(self):
        raw = self._make_openai_response(
            tool_calls=[
                {
                    "id": "call-abc",
                    "type": "function",
                    "function": {
                        "name": "classify_pipeline",
                        "arguments": json.dumps({"has_streaming_origin": True, "stage_count": 5, "execution_mode": "CONTINUOUS"}),
                    },
                }
            ]
        )
        resp = _openai_response_to_anthropic(raw)
        assert resp.stop_reason == "tool_use"
        tool_block = resp.content[0]
        assert tool_block.type == "tool_use"
        assert tool_block.name == "classify_pipeline"
        assert tool_block.input["has_streaming_origin"] is True

    def test_invalid_json_arguments_defaults_empty_dict(self):
        raw = self._make_openai_response(
            tool_calls=[
                {
                    "id": "call-bad",
                    "type": "function",
                    "function": {"name": "my_tool", "arguments": "not-valid-json"},
                }
            ]
        )
        resp = _openai_response_to_anthropic(raw)
        assert resp.content[0].input == {}

    def test_text_and_tool_calls_together(self):
        raw = self._make_openai_response(
            content="Looking up stages...",
            tool_calls=[
                {
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "lookup_stage_mapping", "arguments": "{}"},
                }
            ],
        )
        resp = _openai_response_to_anthropic(raw)
        assert resp.stop_reason == "tool_use"
        types = [b.type for b in resp.content]
        assert "text" in types
        assert "tool_use" in types


# ── GatewayClient.messages_create (mocked HTTP) ───────────────────────────────

class TestGatewayClientMessagesCreate:
    @pytest.fixture
    def client(self, mocker):
        """GatewayClient with mocked httpx.AsyncClient."""
        mocker.patch("agent.gateway_client.settings")
        import agent.gateway_client as gw_mod
        gw_mod.settings.ai_gateway_url = "https://mock-gateway.test/v1"
        gw_mod.settings.ai_gateway_token = "test-token"
        gw_mod.settings.ai_gateway_model = "test-model"
        gw_mod.settings.agent_max_tokens = 4096
        return GatewayClient(
            url="https://mock-gateway.test/v1",
            token="test-token",
            model="test-model",
        )

    async def test_successful_tool_use_response(self, client, mocker):
        openai_response = {
            "choices": [
                {
                    "message": {
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call-1",
                                "type": "function",
                                "function": {
                                    "name": "classify_pipeline",
                                    "arguments": json.dumps({
                                        "has_streaming_origin": True,
                                        "stage_count": 3,
                                        "execution_mode": "CONTINUOUS",
                                    }),
                                },
                            }
                        ],
                    },
                    "finish_reason": "tool_calls",
                }
            ]
        }
        mock_resp = mocker.MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = openai_response
        mock_resp.raise_for_status = mocker.MagicMock()
        client._http.post = mocker.AsyncMock(return_value=mock_resp)

        result = await client.messages_create(
            messages=[{"role": "user", "content": "test"}],
            system="sys",
            tools=[{"name": "classify_pipeline", "input_schema": {"type": "object", "properties": {}}}],
        )
        assert result.stop_reason == "tool_use"
        assert result.content[0].name == "classify_pipeline"

    async def test_retries_on_429(self, client, mocker):
        mocker.patch("agent.gateway_client.asyncio.sleep", new=mocker.AsyncMock())

        rate_limited = mocker.MagicMock()
        rate_limited.status_code = 429
        rate_limited.raise_for_status = mocker.MagicMock(side_effect=Exception("rate limited"))

        ok_response = {
            "choices": [{"message": {"content": "done", "tool_calls": []}, "finish_reason": "stop"}]
        }
        success = mocker.MagicMock()
        success.status_code = 200
        success.json.return_value = ok_response
        success.raise_for_status = mocker.MagicMock()

        client._http.post = mocker.AsyncMock(side_effect=[rate_limited, success])

        result = await client.messages_create(
            messages=[{"role": "user", "content": "test"}],
            system="",
            tools=[],
        )
        assert result.stop_reason == "end_turn"
        assert client._http.post.call_count == 2
