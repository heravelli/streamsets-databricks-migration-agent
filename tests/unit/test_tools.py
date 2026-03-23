"""Unit tests for agent/tools/ — classify_pipeline, lookup_stage_mapping, emit_migration_result."""
import pytest
import yaml
from pathlib import Path

from agent.tools import classify_pipeline, lookup_stage_mapping, emit_migration_result
from catalog.stage_catalog import StageCatalog


# ── classify_pipeline.execute ─────────────────────────────────────────────────

class TestClassifyPipeline:
    def test_streaming_origin_recommends_dlt(self):
        result = classify_pipeline.execute(
            has_streaming_origin=True,
            stage_count=3,
            execution_mode="CONTINUOUS",
        )
        assert result["recommended_format"] == "dlt"
        assert result["confidence"] == "high"

    def test_cdc_origin_recommends_dlt(self):
        result = classify_pipeline.execute(
            has_streaming_origin=False,
            has_cdc_origin=True,
            stage_count=4,
            execution_mode="STANDALONE",
        )
        assert result["recommended_format"] == "dlt"

    def test_simple_batch_recommends_job(self):
        result = classify_pipeline.execute(
            has_streaming_origin=False,
            stage_count=3,
            execution_mode="STANDALONE",
        )
        assert result["recommended_format"] == "job"
        assert result["confidence"] == "high"

    def test_high_stage_count_recommends_notebook(self):
        result = classify_pipeline.execute(
            has_streaming_origin=False,
            stage_count=20,
            execution_mode="STANDALONE",
        )
        assert result["recommended_format"] == "notebook"

    def test_custom_code_stages_recommends_notebook(self):
        result = classify_pipeline.execute(
            has_streaming_origin=False,
            stage_count=5,
            execution_mode="STANDALONE",
            has_custom_code_stages=True,
        )
        assert result["recommended_format"] == "notebook"

    def test_complex_branching_recommends_notebook(self):
        result = classify_pipeline.execute(
            has_streaming_origin=False,
            stage_count=5,
            execution_mode="STANDALONE",
            max_output_lanes=4,
        )
        assert result["recommended_format"] == "notebook"

    def test_many_unsupported_stages_recommends_notebook(self):
        result = classify_pipeline.execute(
            has_streaming_origin=False,
            stage_count=5,
            execution_mode="STANDALONE",
            unsupported_stage_count=3,
        )
        assert result["recommended_format"] == "notebook"

    def test_reasoning_field_present(self):
        result = classify_pipeline.execute(
            has_streaming_origin=True,
            stage_count=3,
            execution_mode="CONTINUOUS",
        )
        assert "reasoning" in result
        assert len(result["reasoning"]) > 0

    def test_streaming_takes_precedence_over_complexity(self):
        """Even a very complex pipeline should be DLT if it has a streaming origin."""
        result = classify_pipeline.execute(
            has_streaming_origin=True,
            stage_count=30,
            execution_mode="CONTINUOUS",
            has_custom_code_stages=True,
        )
        assert result["recommended_format"] == "dlt"


# ── lookup_stage_mapping.execute ──────────────────────────────────────────────

def _make_catalog(tmp_path: Path, stages: list[dict]) -> StageCatalog:
    path = tmp_path / "origins.yaml"
    path.write_text(yaml.dump({"stages": stages}))
    return StageCatalog(tmp_path)


KAFKA_STAGE_DEF = {
    "streamsets_stage": "com.streamsets.pipeline.stage.origin.kafka.KafkaDSource",
    "streamsets_label": "Kafka Consumer",
    "databricks_equivalent": "kafka_streaming",
    "databricks_label": "Kafka Streaming DLT",
    "confidence": "exact",
    "code_template": "spark.readStream.format('kafka')...",
    "config_mapping": {"kafkaConfigBean.metadataBrokerList": "bootstrap_servers"},
    "notes": "Use DLT",
    "requires_manual_review": False,
}


class TestLookupStageMappingTool:
    def test_found_stage_returns_full_info(self, tmp_path):
        catalog = _make_catalog(tmp_path, [KAFKA_STAGE_DEF])
        result = lookup_stage_mapping.execute(
            stage_name="com.streamsets.pipeline.stage.origin.kafka.KafkaDSource",
            stage_config_summary={},
            catalog=catalog,
        )
        assert result["found"] is True
        assert result["confidence"] == "exact"
        assert result["databricks_equivalent"] == "kafka_streaming"
        assert result["requires_manual_review"] is False

    def test_unknown_stage_returns_not_found(self, tmp_path):
        catalog = _make_catalog(tmp_path, [KAFKA_STAGE_DEF])
        result = lookup_stage_mapping.execute(
            stage_name="com.no.mapping.StrangeDSource",
            stage_config_summary={},
            catalog=catalog,
        )
        assert result["found"] is False
        assert result["confidence"] == "unsupported"
        assert "suggestion" in result

    def test_result_has_code_template(self, tmp_path):
        catalog = _make_catalog(tmp_path, [KAFKA_STAGE_DEF])
        result = lookup_stage_mapping.execute(
            stage_name="com.streamsets.pipeline.stage.origin.kafka.KafkaDSource",
            stage_config_summary={},
            catalog=catalog,
        )
        assert "code_template" in result
        assert "kafka" in result["code_template"].lower()

    def test_result_has_config_mapping(self, tmp_path):
        catalog = _make_catalog(tmp_path, [KAFKA_STAGE_DEF])
        result = lookup_stage_mapping.execute(
            stage_name="com.streamsets.pipeline.stage.origin.kafka.KafkaDSource",
            stage_config_summary={},
            catalog=catalog,
        )
        assert "bootstrap_servers" in result["config_mapping"].values()

    def test_real_catalog_kafka_lookup(self, catalog_dir):
        catalog = StageCatalog(catalog_dir)
        result = lookup_stage_mapping.execute(
            stage_name="com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource",
            stage_config_summary={},
            catalog=catalog,
        )
        assert result["found"] is True


# ── emit_migration_result TOOL_SCHEMA ─────────────────────────────────────────

class TestEmitMigrationResultSchema:
    def test_schema_has_required_fields(self):
        schema = emit_migration_result.TOOL_SCHEMA
        required = schema["input_schema"]["required"]
        assert "target_format" in required
        assert "python_code" in required

    def test_schema_name(self):
        assert emit_migration_result.TOOL_SCHEMA["name"] == "emit_migration_result"

    def test_schema_has_target_format_enum(self):
        props = emit_migration_result.TOOL_SCHEMA["input_schema"]["properties"]
        assert "target_format" in props
        assert "enum" in props["target_format"]
        assert "dlt" in props["target_format"]["enum"]
        assert "job" in props["target_format"]["enum"]
        assert "notebook" in props["target_format"]["enum"]


# ── classify_pipeline TOOL_SCHEMA ─────────────────────────────────────────────

class TestClassifyPipelineSchema:
    def test_schema_required_fields(self):
        required = classify_pipeline.TOOL_SCHEMA["input_schema"]["required"]
        assert "has_streaming_origin" in required
        assert "stage_count" in required
        assert "execution_mode" in required

    def test_schema_name(self):
        assert classify_pipeline.TOOL_SCHEMA["name"] == "classify_pipeline"


# ── lookup_stage_mapping TOOL_SCHEMA ──────────────────────────────────────────

class TestLookupStageMappingSchema:
    def test_schema_required_fields(self):
        required = lookup_stage_mapping.TOOL_SCHEMA["input_schema"]["required"]
        assert "stage_name" in required

    def test_schema_name(self):
        assert lookup_stage_mapping.TOOL_SCHEMA["name"] == "lookup_stage_mapping"
