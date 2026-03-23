"""Unit tests for agent/context_builder.py"""
import pytest

from agent.context_builder import build_migration_prompt
from models.migration import GeneratedArtifact, DatabricksTargetFormat
from parser.pipeline_parser import parse_pipeline


@pytest.fixture
def kafka_pipeline(kafka_pipeline_dict):
    return parse_pipeline(kafka_pipeline_dict)


@pytest.fixture
def groovy_pipeline(groovy_pipeline_dict):
    return parse_pipeline(groovy_pipeline_dict)


@pytest.fixture
def sample_artifact():
    return GeneratedArtifact(
        artifact_type=DatabricksTargetFormat.DLT,
        filename="pipeline.py",
        content="@dlt.table\ndef orders(): ...",
        agent_reasoning="Streaming pipeline",
        confidence_score=0.9,
    )


class TestBuildMigrationPromptFull:
    def test_contains_pipeline_title(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "Kafka Orders to Delta Lake" in prompt

    def test_contains_pipeline_id(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "pipeline-kafka-001" in prompt

    def test_contains_execution_mode(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "CONTINUOUS" in prompt

    def test_contains_stage_count(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "3" in prompt  # 3 stages

    def test_contains_stage_instance_names(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "KafkaConsumer_01" in prompt
        assert "FieldTypeConverter_01" in prompt
        assert "HiveMetastore_01" in prompt

    def test_contains_topology_section(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "Pipeline Topology" in prompt

    def test_contains_stage_inventory(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "Stage Inventory" in prompt

    def test_ends_with_migrate_instruction(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "Migrate this pipeline to Databricks" in prompt

    def test_no_feedback_section_without_feedback(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline)
        assert "Reviewer Feedback" not in prompt


class TestBuildMigrationPromptCompact:
    def test_compact_mode_has_stages_section(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline, compact=True)
        assert "### Stages" in prompt

    def test_compact_mode_no_topology(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline, compact=True)
        assert "Pipeline Topology" not in prompt

    def test_compact_mode_no_stage_inventory(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline, compact=True)
        assert "Stage Inventory" not in prompt

    def test_compact_mode_shorter_than_full(self, kafka_pipeline):
        full = build_migration_prompt(kafka_pipeline, compact=False)
        compact = build_migration_prompt(kafka_pipeline, compact=True)
        assert len(compact) < len(full)

    def test_compact_mode_still_has_title(self, kafka_pipeline):
        prompt = build_migration_prompt(kafka_pipeline, compact=True)
        assert "Kafka Orders to Delta Lake" in prompt


class TestBuildMigrationPromptWithFeedback:
    def test_feedback_section_present(self, kafka_pipeline, sample_artifact):
        prompt = build_migration_prompt(
            kafka_pipeline,
            feedback="Please use DLT instead of Spark Streaming",
            prior_artifact=sample_artifact,
        )
        assert "Reviewer Feedback" in prompt
        assert "Re-migration" in prompt

    def test_feedback_text_included(self, kafka_pipeline, sample_artifact):
        feedback_text = "Use DLT for streaming ingestion"
        prompt = build_migration_prompt(
            kafka_pipeline,
            feedback=feedback_text,
            prior_artifact=sample_artifact,
        )
        assert feedback_text in prompt

    def test_prior_code_included(self, kafka_pipeline, sample_artifact):
        prompt = build_migration_prompt(
            kafka_pipeline,
            feedback="Update the code",
            prior_artifact=sample_artifact,
        )
        assert sample_artifact.content in prompt

    def test_no_migrate_instruction_when_feedback_present(self, kafka_pipeline, sample_artifact):
        prompt = build_migration_prompt(
            kafka_pipeline,
            feedback="Fix this",
            prior_artifact=sample_artifact,
        )
        assert "Migrate this pipeline to Databricks" not in prompt

    def test_feedback_without_artifact_shows_no_feedback_section(self, kafka_pipeline):
        # feedback without prior_artifact — the condition requires both
        prompt = build_migration_prompt(kafka_pipeline, feedback="Some feedback")
        assert "Reviewer Feedback" not in prompt


class TestBuildMigrationPromptEdgeCases:
    def test_groovy_pipeline_has_custom_code_flag(self, groovy_pipeline):
        prompt = build_migration_prompt(groovy_pipeline)
        # has_custom_code_stages should be True for Groovy pipeline
        assert "True" in prompt

    def test_empty_stages_pipeline(self):
        from models.streamsets import StreamSetsPipeline
        empty_pipeline = StreamSetsPipeline(
            pipelineId="empty-001",
            title="Empty",
            executionMode="STANDALONE",
        )
        prompt = build_migration_prompt(empty_pipeline)
        assert "Empty" in prompt
        assert "0" in prompt  # 0 stages
