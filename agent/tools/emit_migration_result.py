"""Tool: emit_migration_result — final artifact emission by the agent."""

TOOL_SCHEMA = {
    "name": "emit_migration_result",
    "description": (
        "Emit the final migration result. Call this EXACTLY ONCE at the end, "
        "after all stage lookups and code generation are complete."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "target_format": {
                "type": "string",
                "enum": ["dlt", "job", "notebook"],
                "description": "The Databricks target format for this pipeline",
            },
            "python_code": {
                "type": "string",
                "description": "Complete, runnable Python code for the Databricks artifact",
            },
            "agent_reasoning": {
                "type": "string",
                "description": "Explanation of key migration decisions (format choice, stage mappings, warnings)",
            },
            "confidence_score": {
                "type": "number",
                "minimum": 0,
                "maximum": 1,
                "description": "Aggregate confidence score across all stage mappings (0.0–1.0)",
            },
            "unmapped_stages": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of stage names that had no catalog entry",
            },
            "warnings": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of migration warnings or items requiring manual review",
            },
        },
        "required": ["target_format", "python_code", "agent_reasoning", "confidence_score"],
    },
}
