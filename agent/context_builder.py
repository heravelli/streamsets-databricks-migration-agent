"""
Builds the per-pipeline user prompt sent to the LLM on each migration call.

Two modes controlled by settings.agent_compact_context:
  - Full mode  (default): ~3000-4000 tokens. Includes stage configs and topology.
  - Compact mode (True) : ~1000-1500 tokens. Suitable for token-limited gateway models
                          (e.g. Sonnet via AI gateway with per-request token caps).
"""

from models.streamsets import StreamSetsPipeline, StageDefinition
from models.migration import GeneratedArtifact


def _stage_row_full(stage: StageDefinition) -> str:
    """Full mode: include up to 3 key config values per stage."""
    key_configs = []
    for cfg in stage.configuration[:5]:
        if cfg.value is not None and str(cfg.value).strip():
            val = str(cfg.value)
            if len(val) > 60:
                val = val[:57] + "..."
            key_configs.append(f"{cfg.name}={val}")
    config_str = " | ".join(key_configs[:3]) if key_configs else "-"
    return f"| {stage.instance_name} | {stage.short_name} | {stage.type.value} | {config_str} |"


def _stage_row_compact(stage: StageDefinition) -> str:
    """Compact mode: stage FQCN and type only — no config values."""
    return f"| {stage.instance_name} | {stage.short_name} | {stage.type.value} |"


def build_migration_prompt(
    pipeline: StreamSetsPipeline,
    feedback: str | None = None,
    prior_artifact: GeneratedArtifact | None = None,
    compact: bool = False,
) -> str:
    """
    Build the user-turn prompt for the LLM.

    Args:
        pipeline:       Parsed StreamSets pipeline to migrate.
        feedback:       Reviewer feedback for re-migration (optional).
        prior_artifact: Previously generated artifact for re-migration (optional).
        compact:        If True, emit a token-efficient prompt for gateway models
                        with per-request token limits (set via AGENT_COMPACT_CONTEXT=true).
    """
    lines = [
        f"## Pipeline: {pipeline.title}",
        f"**ID**: {pipeline.pipeline_id}",
        f"**Mode**: {pipeline.execution_mode}",
        f"**Stages**: {len(pipeline.stages)}",
        f"**Has CDC**: {pipeline.has_cdc_origin}",
        f"**Has Kafka**: {pipeline.has_kafka_origin}",
        f"**Has Custom Code**: {pipeline.has_custom_code_stages}",
        "",
    ]

    if compact:
        # ── Compact mode: minimal table, no topology ───────────────────────────
        lines += [
            "### Stages",
            "| Instance | Stage | Type |",
            "|---|---|---|",
        ]
        for stage in pipeline.stages:
            lines.append(_stage_row_compact(stage))
    else:
        # ── Full mode: config values + topology ───────────────────────────────
        lines += [
            "### Stage Inventory",
            "| Instance Name | Stage Type | Lane Type | Key Config |",
            "|---|---|---|---|",
        ]
        for stage in pipeline.stages:
            lines.append(_stage_row_full(stage))

        lines += [
            "",
            "### Pipeline Topology",
            "```",
            pipeline.build_topology(),
            "```",
        ]

    if feedback and prior_artifact:
        lines += [
            "",
            "---",
            "## Reviewer Feedback (Re-migration)",
            "",
            f"> {feedback}",
            "",
            "## Previously Generated Code",
            "```python",
            prior_artifact.content,
            "```",
            "",
            "Revise the migration based on the feedback. Only change flagged parts.",
        ]
    else:
        lines += [
            "",
            "---",
            "Migrate this pipeline to Databricks.",
            "Classify first, look up each stage, then generate complete code.",
        ]

    return "\n".join(lines)
