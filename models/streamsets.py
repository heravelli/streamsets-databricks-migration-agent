from enum import Enum
from typing import Any
from pydantic import BaseModel, Field


class StageType(str, Enum):
    ORIGIN = "SOURCE"
    PROCESSOR = "PROCESSOR"
    DESTINATION = "TARGET"
    EXECUTOR = "EXECUTOR"


class StageConfig(BaseModel):
    name: str
    value: Any = None


class StageDefinition(BaseModel):
    instance_name: str = Field(alias="instanceName")
    stage_name: str = Field(alias="stageName")
    library: str = ""
    type: StageType = StageType.PROCESSOR
    configuration: list[StageConfig] = Field(default_factory=list)
    ui_info: dict = Field(default_factory=dict, alias="uiInfo")
    input_lanes: list[str] = Field(default_factory=list, alias="inputLanes")
    output_lanes: list[str] = Field(default_factory=list, alias="outputLanes")
    event_lanes: list[str] = Field(default_factory=list, alias="eventLanes")

    model_config = {"populate_by_name": True}

    def get_config(self, key: str) -> Any:
        for cfg in self.configuration:
            if cfg.name == key:
                return cfg.value
        return None

    @property
    def short_name(self) -> str:
        return self.stage_name.split(".")[-1]


class StreamSetsPipeline(BaseModel):
    pipeline_id: str = Field(alias="pipelineId", default="")
    title: str = ""
    description: str = ""
    stages: list[StageDefinition] = Field(default_factory=list)
    pipeline_parameters: dict = Field(default_factory=dict, alias="parameters")
    error_stage: StageDefinition | None = Field(default=None, alias="errorStage")
    execution_mode: str = Field(default="STANDALONE", alias="executionMode")
    raw: dict = Field(default_factory=dict, exclude=True)

    model_config = {"populate_by_name": True}

    @property
    def stage_names(self) -> list[str]:
        return [s.stage_name for s in self.stages]

    @property
    def origin_stages(self) -> list[StageDefinition]:
        return [s for s in self.stages if s.type == StageType.ORIGIN]

    @property
    def processor_stages(self) -> list[StageDefinition]:
        return [s for s in self.stages if s.type == StageType.PROCESSOR]

    @property
    def destination_stages(self) -> list[StageDefinition]:
        return [s for s in self.stages if s.type == StageType.DESTINATION]

    @property
    def has_cdc_origin(self) -> bool:
        cdc_keywords = {"cdc", "debezium", "change", "binlog", "wal", "oplog"}
        return any(
            any(kw in s.stage_name.lower() for kw in cdc_keywords)
            for s in self.origin_stages
        )

    @property
    def has_kafka_origin(self) -> bool:
        return any("kafka" in s.stage_name.lower() for s in self.origin_stages)

    @property
    def has_custom_code_stages(self) -> bool:
        custom_keywords = {"groovy", "jython", "javascript", "script"}
        return any(
            any(kw in s.stage_name.lower() for kw in custom_keywords)
            for s in self.stages
        )

    @property
    def output_lane_count(self) -> int:
        return max((len(s.output_lanes) for s in self.stages), default=0)

    def build_topology(self) -> str:
        """Return a human-readable pipeline topology string."""
        lane_to_stage: dict[str, str] = {}
        for stage in self.stages:
            for lane in stage.output_lanes:
                lane_to_stage[lane] = stage.instance_name

        lines = []
        for stage in self.stages:
            inputs = [lane_to_stage.get(lane, "?") for lane in stage.input_lanes]
            from_str = " + ".join(inputs) if inputs else "START"
            lines.append(f"{from_str} → {stage.instance_name} [{stage.short_name}]")
        return "\n".join(lines)
