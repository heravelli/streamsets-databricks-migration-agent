"""Unit tests for agent/client_factory.py"""
import pytest


class TestCreateClient:
    def test_returns_claude_client_by_default(self, mocker):
        mocker.patch("agent.client_factory.settings")
        import agent.client_factory as cf_mod
        cf_mod.settings.agent_client_type = "anthropic"

        mock_client = mocker.MagicMock()
        mocker.patch("agent.claude_client.ClaudeClient", return_value=mock_client)

        from agent.client_factory import create_client
        # Re-import to pick up mocked settings
        import importlib
        import agent.client_factory
        importlib.reload(agent.client_factory)
        agent.client_factory.settings.agent_client_type = "anthropic"

        with mocker.patch("agent.claude_client.ClaudeClient", return_value=mock_client):
            client = agent.client_factory.create_client()
        assert client is mock_client

    def test_returns_gateway_client_for_gateway(self, mocker):
        import agent.client_factory as cf_mod
        mock_gateway = mocker.MagicMock()
        mocker.patch.object(cf_mod, "settings")
        cf_mod.settings.agent_client_type = "gateway"
        cf_mod.settings.ai_gateway_url = "https://gw.test/v1"
        cf_mod.settings.ai_gateway_token = "tok"
        cf_mod.settings.ai_gateway_model = "model"

        mocker.patch("agent.gateway_client.GatewayClient", return_value=mock_gateway)
        with mocker.patch("agent.gateway_client.GatewayClient", return_value=mock_gateway):
            client = cf_mod.create_client()
        assert client is mock_gateway

    def test_openai_raises_without_api_key(self, mocker):
        import agent.client_factory as cf_mod
        mocker.patch.object(cf_mod, "settings")
        cf_mod.settings.agent_client_type = "openai"
        cf_mod.settings.openai_api_key = ""

        with pytest.raises(ValueError, match="OPENAI_API_KEY"):
            cf_mod.create_client()

    def test_openai_uses_gateway_client_with_openai_url(self, mocker):
        import agent.client_factory as cf_mod
        mock_gateway = mocker.MagicMock()
        mocker.patch.object(cf_mod, "settings")
        cf_mod.settings.agent_client_type = "openai"
        cf_mod.settings.openai_api_key = "sk-test-key"
        cf_mod.settings.openai_model = "gpt-4o"

        captured_kwargs = {}

        def capture_gateway(**kwargs):
            captured_kwargs.update(kwargs)
            return mock_gateway

        with mocker.patch("agent.gateway_client.GatewayClient", side_effect=capture_gateway):
            client = cf_mod.create_client()

        assert client is mock_gateway
        assert captured_kwargs.get("url") == "https://api.openai.com/v1"
        assert captured_kwargs.get("token") == "sk-test-key"
        assert captured_kwargs.get("model") == "gpt-4o"

    def test_unknown_client_type_falls_back_to_anthropic(self, mocker):
        """Unknown AGENT_CLIENT_TYPE should fall back to the Anthropic client."""
        import agent.client_factory as cf_mod
        mock_client = mocker.MagicMock()
        mocker.patch.object(cf_mod, "settings")
        cf_mod.settings.agent_client_type = "not-a-valid-type"

        with mocker.patch("agent.claude_client.ClaudeClient", return_value=mock_client):
            client = cf_mod.create_client()
        assert client is mock_client
