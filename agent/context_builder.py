from models.streamsets import StreamSetsPipeline, StageDefinition
from models.migration import GeneratedArtifact


def _stage_row(stage: StageDefinition) -> str:
    key_configs = []
    important_keys = {
        "hikariConfigBean.connectionString", "kafkaConfigBean.metadataBrokerList",
        "kafkaConfigBean.topic", "s3ConfigBean.s3Config.bucket", "dataFormatConfig.dataFormat",
        "conf.resourceUrl", "tableJdbcConfigBean.schema", "groovyScript",
        "mongoConfig.database", "mongoConfig.collection",
    }
    for cfg in stage.configuration[:5]:
        if cfg.value is not None and str(cfg.value).strip():
            val = str(cfg.value)
            if len(val) > 60:
                val = val[:57] + "..."
            key_configs.append(f"{cfg.name}={val}")
    config_str = " | ".join(key_configs[:3]) if key_configs else "-"
    return f"| {stage.instance_name} | {stage.short_name} | {stage.type.value} | {config_str} |"


def build_migration_prompt(
    pipeline: StreamSetsPipeline,
    feedback: str | None = None,
    prior_artifact: GeneratedArtifact | None = None,
) -> str:
    lines = [
        f"## Pipeline: {pipeline.title}",
        f"**Pipeline ID**: {pipeline.pipeline_id}",
        f"**Execution Mode**: {pipeline.execution_mode}",
        f"**Stage Count**: {len(pipeline.stages)}",
        f"**Has CDC Origin**: {pipeline.has_cdc_origin}",
        f"**Has Kafka Origin**: {pipeline.has_kafka_origin}",
        f"**Has Custom Code**: {pipeline.has_custom_code_stages}",
        "",
        "### Stage Inventory",
        "| Instance Name | Stage Type | Lane Type | Key Config |",
        "|---|---|---|---|",
    ]
    for stage in pipeline.stages:
        lines.append(_stage_row(stage))

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
            "## Reviewer Feedback (Re-migration Request)",
            f"The reviewer has requested the following changes:",
            "",
            f"> {feedback}",
            "",
            "## Previously Generated Code",
            f"```python",
            prior_artifact.content,
            "```",
            "",
            "Please revise the migration based on the reviewer's feedback above.",
            "Only change the parts the reviewer flagged — keep the rest intact.",
        ]
    else:
        lines += [
            "",
            "---",
            "Please migrate this StreamSets pipeline to Databricks.",
            "Follow the process: classify first, look up each stage, then generate complete code.",
        ]

    return "\n".join(lines)
