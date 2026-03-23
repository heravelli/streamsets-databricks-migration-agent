"""
MigrationAgent — orchestrates the LLM-driven pipeline migration loop.

LLM USAGE SUMMARY
-----------------
This module makes LLM calls. Every call to `self.client.messages_create()` sends a
request to the configured AI model (direct Anthropic or enterprise AI gateway).

  Client selection:  controlled by AGENT_CLIENT_TYPE env var
    "anthropic" → claude_client.ClaudeClient  (Anthropic SDK, direct)
    "gateway"   → gateway_client.GatewayClient (OpenAI-compatible AI gateway)

  Model:
    Anthropic: ANTHROPIC_MODEL (default: claude-sonnet-4-6)
    Gateway:   AI_GATEWAY_MODEL (as configured by your gateway team)

  Per pipeline:  Up to 20 LLM calls maximum (one per agentic loop iteration).
  Typical flow:  3-5 calls (classify → N×lookup → emit).

  Token budget:  AGENT_MAX_TOKENS per call (default 8096).
                 Set AGENT_COMPACT_CONTEXT=true to reduce input tokens ~60%
                 when using a token-limited gateway model.
"""

import json
from pathlib import Path

from agent.claude_client import ClaudeClient
from agent.context_builder import build_migration_prompt
from agent.tools import classify_pipeline, emit_migration_result, lookup_stage_mapping
from catalog.stage_catalog import StageCatalog
from config.settings import settings
from models.migration import DatabricksTargetFormat, GeneratedArtifact
from models.streamsets import StreamSetsPipeline


class AgentError(Exception):
    pass


class MigrationAgent:
    def __init__(self, client: ClaudeClient, catalog: StageCatalog):
        self.client = client
        self.catalog = catalog
        self._tools = [
            classify_pipeline.TOOL_SCHEMA,
            lookup_stage_mapping.TOOL_SCHEMA,
            emit_migration_result.TOOL_SCHEMA,
        ]
        self._system_prompt = self._load_system_prompt()
        # Use compact context when configured (saves ~60% input tokens for gateway models)
        self._compact = settings.agent_compact_context

    def _load_system_prompt(self) -> str:
        prompt_path = settings.prompts_dir / "system_prompt.md"
        if prompt_path.exists():
            return prompt_path.read_text()
        raise FileNotFoundError(f"System prompt not found at {prompt_path}")

    async def migrate_pipeline(
        self,
        pipeline: StreamSetsPipeline,
        feedback: str | None = None,
        prior_artifact: GeneratedArtifact | None = None,
    ) -> GeneratedArtifact:
        """
        Run the agentic migration loop for a single pipeline.

        [LLM] Calls the configured AI model (Anthropic or gateway) up to 20 times.
        Each iteration processes tool calls (classify_pipeline, lookup_stage_mapping,
        emit_migration_result) until the agent emits a final result.

        Args:
            pipeline:       The parsed StreamSets pipeline to migrate.
            feedback:       Reviewer feedback string for re-migration (optional).
            prior_artifact: Previously generated artifact for re-migration (optional).

        Returns:
            GeneratedArtifact with the complete Databricks Python code.
        """
        context = build_migration_prompt(
            pipeline, feedback, prior_artifact, compact=self._compact
        )
        messages: list[dict] = [{"role": "user", "content": context}]
        final_result: dict | None = None

        for iteration in range(20):  # max iterations guard — prevents runaway LLM loops
            # ── LLM CALL ──────────────────────────────────────────────────────
            # Sends messages + system prompt + tool schemas to the AI model.
            # The model responds by calling one or more tools, or by ending its turn.
            response = await self.client.messages_create(
                messages=messages,
                system=self._system_prompt,
                tools=self._tools,
            )
            # ── END LLM CALL ──────────────────────────────────────────────────

            if response.stop_reason == "tool_use":
                tool_results = self._dispatch_tools(response.content)
                # Capture emit_migration_result if it appeared in this turn
                for block in response.content:
                    if hasattr(block, "name") and block.name == "emit_migration_result":
                        final_result = block.input

                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})

            elif response.stop_reason == "end_turn":
                # Agent finished without an outstanding tool call
                if final_result is None:
                    final_result = self._extract_emit_from_history(messages)
                break

            else:
                raise AgentError(f"Unexpected stop reason: {response.stop_reason}")

        if final_result is None:
            raise AgentError(
                f"Agent did not emit a migration result after {iteration + 1} iterations"
            )

        return self._build_artifact(final_result)

    def _dispatch_tools(self, content_blocks) -> list[dict]:
        """Execute tool calls returned by the LLM. No LLM involvement — pure Python logic."""
        results = []
        for block in content_blocks:
            if not hasattr(block, "name"):
                continue
            name = block.name
            inp = block.input or {}

            if name == "lookup_stage_mapping":
                # [NO LLM] Deterministic catalog lookup — no AI call made here
                result = lookup_stage_mapping.execute(
                    stage_name=inp.get("stage_name", ""),
                    stage_config_summary=inp.get("stage_config_summary", {}),
                    catalog=self.catalog,
                )
            elif name == "classify_pipeline":
                # [NO LLM] Deterministic classification heuristic — no AI call made here
                result = classify_pipeline.execute(**inp)
            elif name == "emit_migration_result":
                # [NO LLM] Receipt acknowledgment — the actual result was captured above
                result = {"status": "received", "message": "Migration result recorded."}
            else:
                result = {"error": f"Unknown tool: {name}"}

            results.append({
                "type": "tool_result",
                "tool_use_id": block.id,
                "content": json.dumps(result),
            })
        return results

    def _extract_emit_from_history(self, messages: list[dict]) -> dict | None:
        """Scan message history for a prior emit_migration_result call. No LLM call."""
        for msg in reversed(messages):
            if msg.get("role") != "assistant":
                continue
            content = msg.get("content", [])
            if not isinstance(content, list):
                continue
            for block in content:
                if hasattr(block, "name") and block.name == "emit_migration_result":
                    return block.input
        return None

    def _build_artifact(self, result: dict) -> GeneratedArtifact:
        """Convert the raw emit_migration_result dict to a GeneratedArtifact. No LLM call."""
        fmt_str = result.get("target_format", "notebook")
        try:
            fmt = DatabricksTargetFormat(fmt_str)
        except ValueError:
            fmt = DatabricksTargetFormat.NOTEBOOK

        suffix = {"dlt": "py", "job": "py", "notebook": "py"}.get(fmt_str, "py")
        filename = f"pipeline.{suffix}"

        return GeneratedArtifact(
            artifact_type=fmt,
            filename=filename,
            content=result.get("python_code", ""),
            agent_reasoning=result.get("agent_reasoning", ""),
            confidence_score=float(result.get("confidence_score", 0.0)),
            unmapped_stages=result.get("unmapped_stages", []),
            warnings=result.get("warnings", []),
        )
