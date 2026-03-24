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

# Deterministic confidence values matching the system prompt definition
_CONFIDENCE_VALUES: dict[str, float] = {
    "exact": 1.0,
    "high": 0.8,
    "medium": 0.5,
    "low": 0.2,
    "unsupported": 0.0,
}
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
        _nudged = False  # send at most one reminder to call emit_migration_result
        self._lookup_confidences: list[float] = []   # collected during dispatch; reset per pipeline
        self._unmapped_stage_names: list[str] = []  # stages where catalog returned found=False

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
                # Agent finished — check if emit was already captured
                if final_result is None:
                    final_result = self._extract_emit_from_history(messages)

                if final_result is not None:
                    break

                # Model stopped early without calling emit_migration_result.
                # Send one nudge to remind it, then continue the loop.
                if _nudged:
                    break  # already nudged — give up and let the None check below raise
                _nudged = True
                # Append the assistant's premature end_turn content (may be empty)
                if response.content:
                    messages.append({"role": "assistant", "content": response.content})
                messages.append({
                    "role": "user",
                    "content": (
                        "You have not yet called `emit_migration_result`. "
                        "Please generate the complete Databricks code now and call "
                        "`emit_migration_result` with the full result."
                    ),
                })
                # [LLM] — continuation after nudge, counted in the same iteration budget

            else:
                raise AgentError(f"Unexpected stop reason: {response.stop_reason}")

        if final_result is None:
            raise AgentError(
                f"Agent did not emit a migration result after {iteration + 1} iterations"
            )

        return self._build_artifact(final_result, pipeline=pipeline)

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
                # Track confidence and unmapped stages deterministically — not left to the LLM
                conf_str = result.get("confidence", "unsupported")
                self._lookup_confidences.append(_CONFIDENCE_VALUES.get(conf_str, 0.0))
                if not result.get("found", False):
                    self._unmapped_stage_names.append(inp.get("stage_name", ""))
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

    @staticmethod
    def _ensure_notebook_format(code: str, pipeline: StreamSetsPipeline, fmt_str: str) -> str:
        """
        Guarantee the generated code is a valid Databricks notebook source file.

        Databricks recognises a .py file as a notebook only when the very first
        line is exactly:  # Databricks notebook source

        If the LLM omitted that header (or placed it elsewhere), this method
        inserts it and ensures the opening %md cell has the pipeline metadata.
        The rest of the code is left untouched.
        """
        NOTEBOOK_HEADER = "# Databricks notebook source"

        # Strip any leading blank lines so we can check the first real line
        stripped = code.lstrip("\n")

        if stripped.startswith(NOTEBOOK_HEADER):
            # Already correct — return as-is (preserve LLM formatting)
            return code

        # Build a tidy header cell and prepend it
        fmt_label = {"dlt": "Delta Live Tables", "job": "Databricks Job", "notebook": "Notebook"}.get(fmt_str, fmt_str)
        header = (
            f"{NOTEBOOK_HEADER}\n"
            f"# MAGIC %md\n"
            f"# MAGIC # {pipeline.title}\n"
            f"# MAGIC Migrated from StreamSets &nbsp;|&nbsp; "
            f"Format: **{fmt_label}**\n"
            f"# MAGIC\n"
            f"# MAGIC **Pipeline ID:** `{pipeline.pipeline_id}`\n"
            f"# MAGIC **Description:** {pipeline.description or 'N/A'}\n"
            f"\n"
            f"# COMMAND ----------\n"
            f"\n"
        )
        return header + stripped

    def _build_artifact(self, result: dict, pipeline: StreamSetsPipeline | None = None) -> GeneratedArtifact:
        """Convert the raw emit_migration_result dict to a GeneratedArtifact. No LLM call."""
        fmt_str = result.get("target_format", "notebook")
        try:
            fmt = DatabricksTargetFormat(fmt_str)
        except ValueError:
            fmt = DatabricksTargetFormat.NOTEBOOK

        filename = "pipeline.py"

        # Compute confidence deterministically from catalog lookup results.
        # The LLM's self-reported confidence_score is ignored — it varies between
        # runs for the same pipeline. The catalog confidence levels are fixed.
        if self._lookup_confidences:
            confidence = round(
                sum(self._lookup_confidences) / len(self._lookup_confidences), 2
            )
        else:
            confidence = float(result.get("confidence_score", 0.0))

        code = result.get("python_code", "")
        # Enforce Databricks notebook source format regardless of what the LLM produced
        if pipeline is not None:
            code = self._ensure_notebook_format(code, pipeline, fmt_str)

        return GeneratedArtifact(
            artifact_type=fmt,
            filename=filename,
            content=code,
            agent_reasoning=result.get("agent_reasoning", ""),
            confidence_score=confidence,
            unmapped_stages=self._unmapped_stage_names,
            warnings=result.get("warnings", []),
        )
