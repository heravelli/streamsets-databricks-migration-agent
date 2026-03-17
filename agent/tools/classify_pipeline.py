"""Tool: classify_pipeline — recommend Databricks target format."""

TOOL_SCHEMA = {
    "name": "classify_pipeline",
    "description": (
        "Analyze pipeline characteristics and return a recommended Databricks target format "
        "(dlt, job, or notebook) with detailed reasoning. Call this FIRST before generating code."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "has_streaming_origin": {
                "type": "boolean",
                "description": "True if pipeline has a Kafka, Kinesis, EventHub, or similar streaming origin",
            },
            "has_cdc_origin": {
                "type": "boolean",
                "description": "True if pipeline reads CDC (MySQL BinLog, Debezium, MongoDB Oplog, etc.)",
            },
            "stage_count": {
                "type": "integer",
                "description": "Total number of stages in the pipeline",
            },
            "has_custom_code_stages": {
                "type": "boolean",
                "description": "True if pipeline contains Groovy, Jython, or JavaScript evaluators",
            },
            "execution_mode": {
                "type": "string",
                "description": "StreamSets execution mode: STANDALONE, CLUSTER, EDGE, etc.",
            },
            "max_output_lanes": {
                "type": "integer",
                "description": "Maximum output lanes on any single stage (indicates branching complexity)",
            },
            "unsupported_stage_count": {
                "type": "integer",
                "description": "Number of stages with no catalog mapping",
            },
        },
        "required": ["has_streaming_origin", "stage_count", "execution_mode"],
    },
}


def execute(
    has_streaming_origin: bool,
    stage_count: int,
    execution_mode: str,
    has_cdc_origin: bool = False,
    has_custom_code_stages: bool = False,
    max_output_lanes: int = 1,
    unsupported_stage_count: int = 0,
) -> dict:
    reasons = []

    if has_cdc_origin or has_streaming_origin:
        reasons.append("Streaming/CDC origin present → DLT provides native streaming and APPLY CHANGES support")
        return {"recommended_format": "dlt", "reasoning": "; ".join(reasons), "confidence": "high"}

    is_high_complexity = (
        stage_count > 15
        or has_custom_code_stages
        or max_output_lanes >= 3
        or unsupported_stage_count > 2
    )

    if is_high_complexity:
        if stage_count > 15:
            reasons.append(f"High stage count ({stage_count}) → Notebook for exploratory migration")
        if has_custom_code_stages:
            reasons.append("Custom code stages require manual translation → Notebook")
        if max_output_lanes >= 3:
            reasons.append(f"Complex branching ({max_output_lanes} lanes) → Notebook")
        if unsupported_stage_count > 2:
            reasons.append(f"{unsupported_stage_count} unmapped stages → Notebook for manual review")
        return {"recommended_format": "notebook", "reasoning": "; ".join(reasons), "confidence": "medium"}

    reasons.append("Batch pipeline with standard stages → Databricks Job")
    return {"recommended_format": "job", "reasoning": "; ".join(reasons), "confidence": "high"}
