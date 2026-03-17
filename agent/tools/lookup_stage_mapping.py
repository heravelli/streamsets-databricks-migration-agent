"""Tool: lookup_stage_mapping — catalog lookup for a StreamSets stage."""

TOOL_SCHEMA = {
    "name": "lookup_stage_mapping",
    "description": (
        "Look up the Databricks equivalent for a StreamSets stage by its fully qualified class name. "
        "Returns the code template, confidence level, and config mapping. "
        "Call this for EACH stage in the pipeline before generating code."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "stage_name": {
                "type": "string",
                "description": (
                    "Fully qualified StreamSets stage class name, "
                    "e.g. 'com.streamsets.pipeline.stage.origin.kafka.KafkaDSource'"
                ),
            },
            "stage_config_summary": {
                "type": "object",
                "description": "Key configuration values from the stage (optional, helps disambiguation)",
            },
        },
        "required": ["stage_name"],
    },
}


def execute(stage_name: str, stage_config_summary: dict, catalog) -> dict:
    mapping = catalog.lookup(stage_name)
    if not mapping:
        return {
            "found": False,
            "stage_name": stage_name,
            "suggestion": (
                "No catalog entry found. Generate custom Python code using Spark DataFrame API. "
                "Mark this stage with a TODO comment in the output."
            ),
            "confidence": "unsupported",
        }
    return {
        "found": True,
        "stage_name": stage_name,
        "databricks_equivalent": mapping.databricks_equivalent.value,
        "databricks_label": mapping.databricks_label,
        "confidence": mapping.confidence.value,
        "code_template": mapping.code_template,
        "config_mapping": mapping.config_mapping,
        "notes": mapping.notes,
        "requires_manual_review": mapping.requires_manual_review,
    }
