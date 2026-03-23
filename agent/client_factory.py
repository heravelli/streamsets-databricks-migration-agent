"""
Factory that returns the right AI client based on AGENT_CLIENT_TYPE env var.

  AGENT_CLIENT_TYPE=anthropic  (default) → ClaudeClient   (Anthropic SDK, direct)
  AGENT_CLIENT_TYPE=gateway               → GatewayClient  (enterprise AI gateway, OpenAI-compatible)
  AGENT_CLIENT_TYPE=openai                → GatewayClient  (OpenAI API, api.openai.com)

Import this wherever a client is needed — no other file needs to know which
client is in use.
"""
from config.settings import settings


def create_client():
    """Return the appropriate AI client based on AGENT_CLIENT_TYPE."""
    client_type = settings.agent_client_type.lower()

    if client_type == "gateway":
        from agent.gateway_client import GatewayClient
        return GatewayClient()  # reads AI_GATEWAY_URL / TOKEN / MODEL from settings

    if client_type == "openai":
        # [LLM] Reuses GatewayClient pointed at OpenAI's API (OpenAI-compatible format)
        if not settings.openai_api_key:
            raise ValueError("OPENAI_API_KEY must be set when AGENT_CLIENT_TYPE=openai")
        from agent.gateway_client import GatewayClient
        return GatewayClient(
            url="https://api.openai.com/v1",
            token=settings.openai_api_key,
            model=settings.openai_model,
        )

    # Default: direct Anthropic SDK
    from agent.claude_client import ClaudeClient
    return ClaudeClient()
