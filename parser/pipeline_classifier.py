from models.streamsets import StreamSetsPipeline
from models.migration import DatabricksTargetFormat


STREAMING_STAGE_KEYWORDS = {"kafka", "kinesis", "pubsub", "eventhub", "rabbitmq", "mqtt"}
CDC_STAGE_KEYWORDS = {"cdc", "debezium", "binlog", "oplog", "wal", "changedata"}
CUSTOM_CODE_KEYWORDS = {"groovy", "jython", "javascript", "scripting"}

STREAMING_EXECUTION_MODES = {"CONTINUOUS", "STREAMING"}


class ClassificationResult:
    def __init__(
        self,
        recommended_format: DatabricksTargetFormat,
        reasoning: str,
        is_streaming: bool,
        is_cdc: bool,
        has_custom_code: bool,
        stage_count: int,
        low_confidence_indicators: list[str],
    ):
        self.recommended_format = recommended_format
        self.reasoning = reasoning
        self.is_streaming = is_streaming
        self.is_cdc = is_cdc
        self.has_custom_code = has_custom_code
        self.stage_count = stage_count
        self.low_confidence_indicators = low_confidence_indicators


def classify_pipeline(pipeline: StreamSetsPipeline) -> ClassificationResult:
    """
    Apply heuristics to determine the best Databricks target format.

    Decision tree:
    1. Streaming/CDC origin → DLT
    2. High complexity (many stages, custom code, many branches) → NOTEBOOK
    3. Default: JOB (batch)
    """
    reasons = []
    low_confidence_indicators = []

    is_streaming = (
        pipeline.execution_mode.upper() in STREAMING_EXECUTION_MODES
        or any(
            any(kw in s.stage_name.lower() for kw in STREAMING_STAGE_KEYWORDS)
            for s in pipeline.origin_stages
        )
    )

    is_cdc = pipeline.has_cdc_origin or any(
        any(kw in s.stage_name.lower() for kw in CDC_STAGE_KEYWORDS)
        for s in pipeline.stages
    )

    has_custom_code = pipeline.has_custom_code_stages
    stage_count = len(pipeline.stages)
    max_output_lanes = pipeline.output_lane_count

    if has_custom_code:
        for s in pipeline.stages:
            if any(kw in s.stage_name.lower() for kw in CUSTOM_CODE_KEYWORDS):
                low_confidence_indicators.append(
                    f"Custom code stage: {s.instance_name} ({s.short_name})"
                )

    # Decision tree
    if is_cdc or is_streaming:
        reasons.append("Pipeline has streaming/CDC origin → DLT is the natural fit")
        if is_cdc:
            reasons.append("CDC origin detected → use DLT APPLY CHANGES for upsert semantics")
        recommended = DatabricksTargetFormat.DLT

    elif (
        stage_count > 15
        or has_custom_code
        or max_output_lanes >= 3
    ):
        if stage_count > 15:
            reasons.append(f"High stage count ({stage_count} > 15) → Notebook for exploratory migration")
        if has_custom_code:
            reasons.append("Custom code stages (Groovy/Jython/JS) → Notebook for manual review")
        if max_output_lanes >= 3:
            reasons.append(f"Complex branching ({max_output_lanes} output lanes) → Notebook")
        recommended = DatabricksTargetFormat.NOTEBOOK

    else:
        reasons.append("Batch pipeline with no streaming/CDC → Databricks Job")
        recommended = DatabricksTargetFormat.JOB

    return ClassificationResult(
        recommended_format=recommended,
        reasoning="; ".join(reasons),
        is_streaming=is_streaming,
        is_cdc=is_cdc,
        has_custom_code=has_custom_code,
        stage_count=stage_count,
        low_confidence_indicators=low_confidence_indicators,
    )
