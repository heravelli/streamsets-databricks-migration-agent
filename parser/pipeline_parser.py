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


def parse_pipeline(raw: dict, source_path: str = "") -> StreamSetsPipeline:
    """Parse a StreamSets pipeline dict (from JSON export) into a StreamSetsPipeline."""
    try:
        stages_raw = raw.get("stages", [])
        stages = [_parse_stage(s) for s in stages_raw]

        error_stage_raw = raw.get("errorStage")
        error_stage = _parse_stage(error_stage_raw) if error_stage_raw else None

        pipeline = StreamSetsPipeline(
            pipelineId=raw.get("pipelineId", raw.get("info", {}).get("pipelineId", Path(source_path).stem)),
            title=raw.get("title", raw.get("info", {}).get("title", Path(source_path).stem)),
            description=raw.get("description", ""),
            stages=stages,
            parameters=raw.get("metadata", {}).get("labels", {}),
            errorStage=error_stage,
            executionMode=raw.get("executionMode", "STANDALONE"),
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
