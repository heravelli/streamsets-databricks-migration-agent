from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # ── Client selection ──────────────────────────────────────────────────────
    # "anthropic" (default) → direct Anthropic SDK
    # "gateway"             → OpenAI-compatible enterprise AI gateway
    agent_client_type: str = "anthropic"

    # ── Direct Anthropic SDK ──────────────────────────────────────────────────
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-6"

    # ── Enterprise AI Gateway ─────────────────────────────────────────────────
    # Set AGENT_CLIENT_TYPE=gateway and fill these in to use the gateway
    ai_gateway_url: str = ""          # e.g. https://ai-gateway.yourcompany.com/v1
    ai_gateway_token: str = ""        # Bearer token from the gateway team
    ai_gateway_model: str = ""        # Model name as exposed by the gateway

    # Completions path appended to AI_GATEWAY_URL.
    # Default "/chat/completions" works for OpenAI-compatible gateways (LiteLLM, Azure, etc.).
    # If your gateway embeds the model in the URL path use "{model}" as a placeholder, e.g.:
    #   AI_GATEWAY_COMPLETIONS_PATH=/chat/{model}
    # The client will substitute the actual model name and POST directly to that URL.
    # When "{model}" is present the "model" field is omitted from the request body.
    ai_gateway_completions_path: str = "/chat/completions"

    # ── OpenAI ────────────────────────────────────────────────────────────────
    # Set AGENT_CLIENT_TYPE=openai to use OpenAI directly with your API key
    openai_api_key: str = ""          # OPENAI_API_KEY
    openai_model: str = "gpt-4o"      # OPENAI_MODEL (gpt-4o recommended for tool use)

    # ── Paths configurable via env vars for container deployments ─────────────
    data_dir: Path = Path("data/pipelines")
    output_dir: Path = Path("output")
    state_file: Path = Path("data/state/migration_state.json")
    catalog_dir: Path = Path("catalog/stages")
    prompts_dir: Path = Path("agent/prompts")

    # Agent settings
    agent_max_tokens: int = 8096
    agent_concurrency: int = 3
    # Set AGENT_COMPACT_CONTEXT=true when using a token-limited gateway model (e.g. Sonnet via AI gateway).
    # Compact mode reduces the per-pipeline prompt from ~4000 tokens to ~1500 tokens by omitting
    # verbose topology and config details, keeping only stage name, type, and FQCN.
    agent_compact_context: bool = False

    # Review portal
    review_host: str = "0.0.0.0"
    review_port: int = 8000


settings = Settings()
