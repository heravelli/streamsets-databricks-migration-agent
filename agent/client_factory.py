"""
Factory that returns the right AI client based on AGENT_CLIENT_TYPE env var.

  AGENT_CLIENT_TYPE=anthropic  (default) → ClaudeClient  (Anthropic SDK, direct)
  AGENT_CLIENT_TYPE=gateway               → GatewayClient (OpenAI-compatible HTTP)

Import this wherever a client is needed — no other file needs to know which
client is in use.
"""
from config.settings import settings


def create_client():
    """Return a ClaudeClient or GatewayClient based on AGENT_CLIENT_TYPE."""
    client_type = settings.agent_client_type.lower()
    if client_type == "gateway":
        from agent.gateway_client import GatewayClient
        return GatewayClient()
    # Default: direct Anthropic SDK
    from agent.claude_client import ClaudeClient
    return ClaudeClient()
