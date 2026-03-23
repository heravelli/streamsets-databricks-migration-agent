"""Unit tests for parser/pipeline_parser.py"""
import json
import pytest
from pathlib import Path

from parser.pipeline_parser import (
    parse_pipeline,
    parse_file,
    scan_directory,
    PipelineParseError,
    _normalize_stage_type,
)
from models.streamsets import StageType


# ── _normalize_stage_type ────────────────────────────────────────────────────

class TestNormalizeStageType:
    def test_source_maps_to_origin(self):
        assert _normalize_stage_type("SOURCE") == StageType.ORIGIN

    def test_origin_maps_to_origin(self):
        assert _normalize_stage_type("ORIGIN") == StageType.ORIGIN

    def test_processor_maps_to_processor(self):
        assert _normalize_stage_type("PROCESSOR") == StageType.PROCESSOR

    def test_target_maps_to_destination(self):
        assert _normalize_stage_type("TARGET") == StageType.DESTINATION

    def test_destination_maps_to_destination(self):
        assert _normalize_stage_type("DESTINATION") == StageType.DESTINATION

    def test_executor_maps_to_executor(self):
        assert _normalize_stage_type("EXECUTOR") == StageType.EXECUTOR

    def test_case_insensitive(self):
        assert _normalize_stage_type("source") == StageType.ORIGIN
        assert _normalize_stage_type("Target") == StageType.DESTINATION

    def test_unknown_defaults_to_processor(self):
        assert _normalize_stage_type("UNKNOWN") == StageType.PROCESSOR


# ── parse_pipeline ───────────────────────────────────────────────────────────

class TestParsePipeline:
    def test_basic_fields(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        assert pipeline.pipeline_id == "pipeline-kafka-001"
        assert pipeline.title == "Kafka Orders to Delta Lake"
        assert pipeline.execution_mode == "CONTINUOUS"

    def test_stage_count(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        assert len(pipeline.stages) == 3

    def test_stage_names(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        names = [s.stage_name for s in pipeline.stages]
        assert "com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource" in names
        assert "com.streamsets.pipeline.stage.processor.fieldtypeconverter.FieldTypeConverterDProcessor" in names

    def test_stage_configurations_parsed(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        kafka_stage = pipeline.stages[0]
        broker = kafka_stage.get_config("conf.brokerURI")
        assert broker == "kafka:9092"

    def test_input_output_lanes(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        kafka_stage = pipeline.stages[0]
        assert kafka_stage.input_lanes == []
        assert "KafkaConsumer_01OutputLane0" in kafka_stage.output_lanes

    def test_no_error_stage(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        assert pipeline.error_stage is None

    def test_raw_dict_attached(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        assert pipeline.raw == kafka_pipeline_dict

    def test_groovy_pipeline(self, groovy_pipeline_dict):
        pipeline = parse_pipeline(groovy_pipeline_dict)
        assert pipeline.pipeline_id == "pipeline-groovy-001"
        assert pipeline.execution_mode == "STANDALONE"
        assert len(pipeline.stages) == 3

    def test_missing_pipeline_id_uses_stem(self):
        raw = {"title": "Test", "stages": []}
        pipeline = parse_pipeline(raw, source_path="/data/my_pipeline.json")
        assert pipeline.pipeline_id == "my_pipeline"

    def test_bad_data_raises_parse_error(self):
        with pytest.raises(PipelineParseError):
            # stage with no name key at all — parser should fail gracefully
            parse_pipeline({"stages": [{"configuration": "not-a-list"}]})


# ── StreamSetsPipeline properties ────────────────────────────────────────────

class TestPipelineProperties:
    def test_has_kafka_origin_true(self, kafka_pipeline_dict):
        # Parser reads uiInfo.stageType; conftest stages have empty uiInfo → all PROCESSOR
        # We need to inject uiInfo.stageType to test origin detection
        kafka_pipeline_dict["stages"][0]["uiInfo"] = {"stageType": "SOURCE"}
        pipeline = parse_pipeline(kafka_pipeline_dict)
        assert pipeline.has_kafka_origin is True

    def test_has_kafka_origin_false(self, groovy_pipeline_dict):
        groovy_pipeline_dict["stages"][0]["uiInfo"] = {"stageType": "SOURCE"}
        pipeline = parse_pipeline(groovy_pipeline_dict)
        assert pipeline.has_kafka_origin is False

    def test_has_custom_code_stages_true(self, groovy_pipeline_dict):
        pipeline = parse_pipeline(groovy_pipeline_dict)
        assert pipeline.has_custom_code_stages is True

    def test_has_custom_code_stages_false(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        assert pipeline.has_custom_code_stages is False

    def test_build_topology(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        topology = pipeline.build_topology()
        assert "KafkaConsumer_01" in topology
        assert "FieldTypeConverter_01" in topology
        assert "HiveMetastore_01" in topology

    def test_stage_names_property(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        assert len(pipeline.stage_names) == 3
        assert "com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource" in pipeline.stage_names

    def test_short_name(self, kafka_pipeline_dict):
        pipeline = parse_pipeline(kafka_pipeline_dict)
        assert pipeline.stages[0].short_name == "MultiKafkaDSource"


# ── parse_file ───────────────────────────────────────────────────────────────

class TestParseFile:
    def test_parse_json_file(self, pipelines_dir, kafka_pipeline_dict):
        path = pipelines_dir / "kafka_to_delta.json"
        pipeline = parse_file(path)
        assert pipeline.pipeline_id == kafka_pipeline_dict["pipelineId"]

    def test_parse_yaml_file(self, tmp_path, groovy_pipeline_dict):
        import yaml
        path = tmp_path / "test.yaml"
        path.write_text(yaml.dump(groovy_pipeline_dict))
        pipeline = parse_file(path)
        assert pipeline.pipeline_id == groovy_pipeline_dict["pipelineId"]

    def test_missing_file_raises_parse_error(self, tmp_path):
        with pytest.raises(PipelineParseError):
            parse_file(tmp_path / "nonexistent.json")

    def test_invalid_json_raises_parse_error(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("not-valid-json{{{")
        with pytest.raises(PipelineParseError):
            parse_file(bad)


# ── scan_directory ───────────────────────────────────────────────────────────

class TestScanDirectory:
    def test_finds_both_files(self, pipelines_dir):
        results = scan_directory(pipelines_dir)
        assert len(results) == 2

    def test_all_succeed(self, pipelines_dir):
        results = scan_directory(pipelines_dir)
        for path, result in results:
            assert not isinstance(result, Exception), f"Parse failed for {path}: {result}"

    def test_returns_correct_pipeline_ids(self, pipelines_dir):
        results = scan_directory(pipelines_dir)
        ids = {r.pipeline_id for _, r in results}
        assert "pipeline-kafka-001" in ids
        assert "pipeline-groovy-001" in ids

    def test_error_file_included_as_exception(self, pipelines_dir):
        (pipelines_dir / "broken.json").write_text("{invalid}")
        results = scan_directory(pipelines_dir)
        errors = [r for _, r in results if isinstance(r, Exception)]
        assert len(errors) == 1

    def test_empty_directory(self, tmp_path):
        results = scan_directory(tmp_path)
        assert results == []
