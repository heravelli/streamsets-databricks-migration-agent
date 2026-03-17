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
        Handles both initial migration and re-migration after reviewer feedback.
        """
        context = build_migration_prompt(pipeline, feedback, prior_artifact)
        messages: list[dict] = [{"role": "user", "content": context}]
        final_result: dict | None = None

        for _ in range(20):  # max iterations guard
            response = await self.client.messages_create(
                messages=messages,
                system=self._system_prompt,
                tools=self._tools,
            )

            if response.stop_reason == "tool_use":
                tool_results = self._dispatch_tools(response.content)
                # Collect emit_migration_result if it appeared
                for block in response.content:
                    if hasattr(block, "name") and block.name == "emit_migration_result":
                        final_result = block.input

                messages.append({"role": "assistant", "content": response.content})
                messages.append({"role": "user", "content": tool_results})

            elif response.stop_reason == "end_turn":
                # Check if emit was in the last turn
                if final_result is None:
                    # Extract from message history as fallback
                    final_result = self._extract_emit_from_history(messages)
                break
            else:
                raise AgentError(f"Unexpected stop reason: {response.stop_reason}")

        if final_result is None:
            raise AgentError("Agent did not emit a migration result")

        return self._build_artifact(final_result)

    def _dispatch_tools(self, content_blocks) -> list[dict]:
        results = []
        for block in content_blocks:
            if not hasattr(block, "name"):
                continue
            name = block.name
            inp = block.input or {}

            if name == "lookup_stage_mapping":
                result = lookup_stage_mapping.execute(
                    stage_name=inp.get("stage_name", ""),
                    stage_config_summary=inp.get("stage_config_summary", {}),
                    catalog=self.catalog,
                )
            elif name == "classify_pipeline":
                result = classify_pipeline.execute(**inp)
            elif name == "emit_migration_result":
                # Acknowledge receipt — agent can continue after this
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
