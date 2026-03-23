import json
from pathlib import Path
import yaml
from models.streamsets import StreamSetsPipeline, StageDefinition, StageType, StageConfig


class PipelineParseError(Exception):
    pass


def _normalize_stage_type(raw_type: str) -> StageType:
    """Map various StreamSets stage type strings to our enum."""
    mapping = {
        "SOURCE": StageType.ORIGIN,
        "ORIGIN": StageType.ORIGIN,
        "PROCESSOR": StageType.PROCESSOR,
        "TARGET": StageType.DESTINATION,
        "DESTINATION": StageType.DESTINATION,
        "EXECUTOR": StageType.EXECUTOR,
    }
    return mapping.get(raw_type.upper(), StageType.PROCESSOR)


def _parse_stage(stage_raw: dict) -> StageDefinition:
    """Parse a single stage dict from StreamSets JSON export."""
    configurations = [
        StageConfig(name=c["name"], value=c.get("value"))
        for c in stage_raw.get("configuration", [])
    ]
    return StageDefinition(
        instanceName=stage_raw.get("instanceName", stage_raw.get("instance_name", "")),
        stageName=stage_raw.get("stageName", stage_raw.get("stage_name", "")),
        library=stage_raw.get("library", ""),
        type=_normalize_stage_type(stage_raw.get("uiInfo", {}).get("stageType", "PROCESSOR")),
        configuration=configurations,
        uiInfo=stage_raw.get("uiInfo", {}),
        inputLanes=stage_raw.get("inputLanes", []),
        outputLanes=stage_raw.get("outputLanes", []),
        eventLanes=stage_raw.get("eventLanes", []),
    )


def _execution_mode_from_config(configuration: list[dict]) -> str:
    """
    Real StreamSets exports store executionMode inside the pipelineConfig.configuration
    array as {"name": "executionMode", "value": "..."}.  Fall back to STANDALONE if absent.
    """
    for entry in configuration:
        if entry.get("name") == "executionMode":
            return str(entry.get("value", "STANDALONE"))
    return "STANDALONE"


def parse_pipeline(raw: dict, source_path: str = "") -> StreamSetsPipeline:
    """
    Parse a StreamSets pipeline dict (from JSON export) into a StreamSetsPipeline.

    Handles both export shapes:
      • Old / simplified:  stages / pipelineId / title at the top level
      • Real SDC export:   everything nested under a "pipelineConfig" key,
                           with a sibling "pipelineRules" key
    """
    try:
        # ── Unwrap pipelineConfig wrapper if present (real SDC export format) ──
        cfg = raw.get("pipelineConfig", raw)  # fall back to raw itself for flat format

        stages_raw = cfg.get("stages", [])
        stages = [_parse_stage(s) for s in stages_raw]

        error_stage_raw = cfg.get("errorStage")
        # errorStage can be a dict (single stage) or a list; handle both
        if isinstance(error_stage_raw, list):
            error_stage_raw = error_stage_raw[0] if error_stage_raw else None
        error_stage = _parse_stage(error_stage_raw) if error_stage_raw else None

        # pipelineId: prefer cfg field, then info sub-object, then filename stem
        pipeline_id = (
            cfg.get("pipelineId")
            or cfg.get("info", {}).get("pipelineId")
            or Path(source_path).stem
        )

        title = (
            cfg.get("title")
            or cfg.get("info", {}).get("title")
            or Path(source_path).stem
        )

        # executionMode may be a top-level field (flat format) or inside the
        # configuration array (real SDC export format)
        execution_mode = cfg.get("executionMode") or _execution_mode_from_config(
            cfg.get("configuration", [])
        )

        # metadata.labels can be a list of tag strings (real SDC export) or
        # a dict of key/value parameters (simplified format). Normalise to dict.
        raw_labels = cfg.get("metadata", {}).get("labels", {})
        if isinstance(raw_labels, list):
            parameters = {label: True for label in raw_labels}
        elif isinstance(raw_labels, dict):
            parameters = raw_labels
        else:
            parameters = {}

        pipeline = StreamSetsPipeline(
            pipelineId=pipeline_id,
            title=title,
            description=cfg.get("description", ""),
            stages=stages,
            parameters=parameters,
            errorStage=error_stage,
            executionMode=execution_mode,
        )
        pipeline.raw = raw
        return pipeline
    except Exception as e:
        raise PipelineParseError(f"Failed to parse pipeline from {source_path}: {e}") from e


def parse_file(path: Path) -> StreamSetsPipeline:
    """Load a StreamSets pipeline export file (JSON or YAML)."""
    try:
        with open(path) as f:
            if path.suffix.lower() in (".yaml", ".yml"):
                raw = yaml.safe_load(f)
            else:
                raw = json.load(f)
        return parse_pipeline(raw, source_path=str(path))
    except PipelineParseError:
        raise
    except Exception as e:
        raise PipelineParseError(f"Failed to read {path}: {e}") from e


def scan_directory(directory: Path) -> list[tuple[Path, StreamSetsPipeline | Exception]]:
    """Scan a directory for pipeline export files and parse all of them."""
    results = []
    for path in sorted(directory.glob("**/*.json")) + sorted(directory.glob("**/*.yaml")) + sorted(directory.glob("**/*.yml")):
        try:
            pipeline = parse_file(path)
            results.append((path, pipeline))
        except PipelineParseError as e:
            results.append((path, e))
    return results
