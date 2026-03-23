"""
Integration tests: full migration cycle — ingest → migrate (mocked LLM) → export.

These tests run the real parser, state manager, catalog, and agent orchestration.
Only the LLM API call is mocked: we inject a pre-canned response that simulates
the agent calling classify_pipeline, lookup_stage_mapping (once per stage), and
emit_migration_result in sequence.
"""
import json
import shutil
from pathlib import Path

import pytest

from catalog.stage_catalog import StageCatalog
from models.migration import PipelineRecord, PipelineStatus, GeneratedArtifact, DatabricksTargetFormat
from parser.pipeline_parser import scan_directory
from state.state_manager import StateManager


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_tool_use_block(name: str, input_: dict, block_id: str = None):
    """Create a mock _ToolUseBlock-like object."""
    from agent.gateway_client import _ToolUseBlock
    import uuid
    return _ToolUseBlock(block_id or str(uuid.uuid4()), name, input_)


def _make_text_block(text: str):
    from agent.gateway_client import _TextBlock
    return _TextBlock(text)


def _make_mock_response(stop_reason: str, content: list):
    from agent.gateway_client import _GatewayResponse
    return _GatewayResponse(stop_reason=stop_reason, content=content)


# ── Ingest tests (CLI-layer logic via direct function call) ───────────────────

class TestIngestCycle:
    def test_ingest_registers_all_pipelines(self, pipelines_dir, state_file):
        sm = StateManager(state_file)
        results = scan_directory(pipelines_dir)
        for path, pipeline in results:
            if isinstance(pipeline, Exception):
                continue
            record = PipelineRecord(
                pipeline_id=pipeline.pipeline_id,
                pipeline_title=pipeline.title,
                team_name="team_alpha",
                source_file_path=str(path.absolute()),
            )
            sm.register_pipeline(record, "team_alpha")

        state = sm.load()
        assert "pipeline-kafka-001" in state.pipelines
        assert "pipeline-groovy-001" in state.pipelines
        assert len(state.team_index["team_alpha"]) == 2

    def test_ingest_sets_pending_status(self, pipelines_dir, state_file):
        sm = StateManager(state_file)
        results = scan_directory(pipelines_dir)
        for path, pipeline in results:
            if isinstance(pipeline, Exception):
                continue
            record = PipelineRecord(
                pipeline_id=pipeline.pipeline_id,
                pipeline_title=pipeline.title,
                team_name="team_alpha",
                source_file_path=str(path.absolute()),
            )
            sm.register_pipeline(record, "team_alpha")

        for record in sm.load().pipelines.values():
            assert record.status == PipelineStatus.PENDING

    def test_ingest_with_team_inference(self, pipelines_dir, state_file):
        """Directory name should be usable as team name."""
        team = pipelines_dir.name  # "team_alpha" from conftest
        sm = StateManager(state_file)
        results = scan_directory(pipelines_dir)
        for path, pipeline in results:
            if not isinstance(pipeline, Exception):
                record = PipelineRecord(
                    pipeline_id=pipeline.pipeline_id,
                    pipeline_title=pipeline.title,
                    team_name=team,
                    source_file_path=str(path.absolute()),
                )
                sm.register_pipeline(record, team)

        assert team in sm.load().team_index


# ── Migration cycle (mocked LLM) ─────────────────────────────────────────────

class TestMigrateCycle:
    @pytest.fixture
    def populated_state(self, pipelines_dir, state_file):
        """State with both sample pipelines ingested."""
        sm = StateManager(state_file)
        results = scan_directory(pipelines_dir)
        for path, pipeline in results:
            if not isinstance(pipeline, Exception):
                record = PipelineRecord(
                    pipeline_id=pipeline.pipeline_id,
                    pipeline_title=pipeline.title,
                    team_name="team_alpha",
                    source_file_path=str(path.absolute()),
                )
                sm.register_pipeline(record, "team_alpha")
        return sm

    async def test_migrate_single_pipeline_mocked_llm(
        self, populated_state, catalog_dir, mocker, state_file
    ):
        """
        Full agent run for the Kafka pipeline with mocked LLM.

        Mock response sequence:
          Turn 1: classify_pipeline tool call
          Turn 2: lookup_stage_mapping × 3 (one per stage)
          Turn 3: emit_migration_result
          Turn 4: end_turn
        """
        from agent.migration_agent import MigrationAgent
        from parser.pipeline_parser import parse_file

        # Load pipeline from state
        sm = StateManager(state_file)
        record = sm.get_pipeline("pipeline-kafka-001")
        pipeline = parse_file(Path(record.source_file_path))
        catalog = StageCatalog(catalog_dir)

        # Build mocked LLM response sequence
        turn1 = _make_mock_response("tool_use", [
            _make_tool_use_block("classify_pipeline", {
                "has_streaming_origin": True,
                "stage_count": 3,
                "execution_mode": "CONTINUOUS",
            }, "call-classify"),
        ])
        turn2 = _make_mock_response("tool_use", [
            _make_tool_use_block("lookup_stage_mapping", {
                "stage_name": "com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource",
            }, "call-lookup-1"),
            _make_tool_use_block("lookup_stage_mapping", {
                "stage_name": "com.streamsets.pipeline.stage.processor.fieldtypeconverter.FieldTypeConverterDProcessor",
            }, "call-lookup-2"),
            _make_tool_use_block("lookup_stage_mapping", {
                "stage_name": "com.streamsets.pipeline.stage.destination.jdbc.JdbcDTarget",
            }, "call-lookup-3"),
        ])
        turn3 = _make_mock_response("tool_use", [
            _make_tool_use_block("emit_migration_result", {
                "target_format": "dlt",
                "python_code": "import dlt\n\n@dlt.table\ndef orders(): pass",
                "agent_reasoning": "Kafka streaming pipeline → DLT streaming table",
                "confidence_score": 0.92,
                "unmapped_stages": [],
                "warnings": [],
            }, "call-emit"),
        ])
        turn4 = _make_mock_response("end_turn", [
            _make_text_block("Migration complete."),
        ])

        mock_client = mocker.MagicMock()
        mock_client.messages_create = mocker.AsyncMock(
            side_effect=[turn1, turn2, turn3, turn4]
        )

        # Patch system prompt loading so we don't need the file in tests
        mocker.patch.object(
            MigrationAgent, "_load_system_prompt", return_value="You are a migration agent."
        )

        agent = MigrationAgent(client=mock_client, catalog=catalog)
        artifact = await agent.migrate_pipeline(pipeline)

        assert artifact.artifact_type == DatabricksTargetFormat.DLT
        assert "dlt.table" in artifact.content
        # Confidence is now computed deterministically from catalog lookups (not LLM output)
        assert 0.0 < artifact.confidence_score <= 1.0
        assert artifact.unmapped_stages == []

    async def test_migrate_groovy_pipeline_recommends_notebook(
        self, populated_state, catalog_dir, mocker, state_file
    ):
        from agent.migration_agent import MigrationAgent
        from parser.pipeline_parser import parse_file

        sm = StateManager(state_file)
        record = sm.get_pipeline("pipeline-groovy-001")
        pipeline = parse_file(Path(record.source_file_path))
        catalog = StageCatalog(catalog_dir)

        turn1 = _make_mock_response("tool_use", [
            _make_tool_use_block("classify_pipeline", {
                "has_streaming_origin": False,
                "stage_count": 3,
                "execution_mode": "STANDALONE",
                "has_custom_code_stages": True,
            }, "call-classify"),
        ])
        turn2 = _make_mock_response("tool_use", [
            _make_tool_use_block("emit_migration_result", {
                "target_format": "notebook",
                "python_code": "# Groovy → Notebook\ndf = spark.read.jdbc(...)",
                "agent_reasoning": "Custom Groovy code requires manual translation → Notebook",
                "confidence_score": 0.6,
                "unmapped_stages": [],
                "warnings": ["GroovyEval_01 requires manual code translation"],
            }, "call-emit"),
        ])
        turn3 = _make_mock_response("end_turn", [])

        mock_client = mocker.MagicMock()
        mock_client.messages_create = mocker.AsyncMock(
            side_effect=[turn1, turn2, turn3]
        )
        mocker.patch.object(
            MigrationAgent, "_load_system_prompt", return_value="You are a migration agent."
        )

        agent = MigrationAgent(client=mock_client, catalog=catalog)
        artifact = await agent.migrate_pipeline(pipeline)

        assert artifact.artifact_type == DatabricksTargetFormat.NOTEBOOK
        assert len(artifact.warnings) == 1


# ── Export cycle ──────────────────────────────────────────────────────────────

class TestExportCycle:
    @pytest.fixture
    def approved_state(self, state_file, tmp_path):
        """State with one approved pipeline and a real artifact on disk."""
        sm = StateManager(state_file)

        artifact = GeneratedArtifact(
            artifact_type=DatabricksTargetFormat.DLT,
            filename="pipeline.py",
            content="import dlt\n\n@dlt.table\ndef orders(): pass",
            agent_reasoning="Kafka → DLT",
            confidence_score=0.95,
        )
        record = PipelineRecord(
            pipeline_id="pipeline-kafka-001",
            pipeline_title="Kafka Orders to Delta Lake",
            team_name="team_alpha",
            source_file_path="/data/team_alpha/kafka.json",
            status=PipelineStatus.APPROVED,
            current_artifact=artifact,
        )
        sm.register_pipeline(record, "team_alpha")

        # Write artifact to output dir (mirrors what migrate run would do)
        output_dir = tmp_path / "output" / "team_alpha" / "pipeline-kafka-001"
        output_dir.mkdir(parents=True)
        (output_dir / "pipeline.py").write_text(artifact.content)
        (output_dir / "migration_metadata.json").write_text('{"confidence": 0.95}')

        return sm, tmp_path / "output"

    def test_only_approved_pipelines_exported(self, approved_state, tmp_path):
        sm, output_root = approved_state
        state = sm.load()

        export_dir = tmp_path / "deploy"
        approved = [r for r in state.pipelines.values() if r.status == PipelineStatus.APPROVED]

        exported_count = 0
        for record in approved:
            src = output_root / record.team_name / record.pipeline_id
            if src.exists():
                dest = export_dir / record.team_name / record.pipeline_id
                dest.mkdir(parents=True, exist_ok=True)
                for f in src.iterdir():
                    shutil.copy2(f, dest / f.name)
                exported_count += 1

        assert exported_count == 1
        assert (export_dir / "team_alpha" / "pipeline-kafka-001" / "pipeline.py").exists()

    def test_export_zip_contains_artifact(self, approved_state, tmp_path):
        import zipfile
        sm, output_root = approved_state
        state = sm.load()

        tmp_bundle = tmp_path / "_export_tmp"
        for record in state.pipelines.values():
            if record.status == PipelineStatus.APPROVED:
                src = output_root / record.team_name / record.pipeline_id
                if src.exists():
                    dest = tmp_bundle / record.team_name / record.pipeline_id
                    dest.mkdir(parents=True, exist_ok=True)
                    for f in src.iterdir():
                        shutil.copy2(f, dest / f.name)

        zip_path = tmp_path / "bundle.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in tmp_bundle.rglob("*"):
                if f.is_file():
                    zf.write(f, f.relative_to(tmp_bundle))

        assert zip_path.exists()
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert any("pipeline.py" in n for n in names)

    def test_pending_pipeline_not_exported(self, approved_state, tmp_path, state_file):
        sm, output_root = approved_state

        # Add a pending pipeline
        pending_record = PipelineRecord(
            pipeline_id="pipeline-groovy-001",
            pipeline_title="Groovy Pipeline",
            team_name="team_alpha",
            source_file_path="/data/team_alpha/groovy.json",
            status=PipelineStatus.PENDING,
        )
        sm.register_pipeline(pending_record, "team_alpha")

        state = sm.load()
        approved = [r for r in state.pipelines.values() if r.status == PipelineStatus.APPROVED]
        assert len(approved) == 1
        assert approved[0].pipeline_id == "pipeline-kafka-001"


# ── Team progress ─────────────────────────────────────────────────────────────

class TestTeamProgress:
    def test_team_progress_counts(self, state_file):
        sm = StateManager(state_file)
        for i in range(5):
            sm.register_pipeline(
                PipelineRecord(
                    pipeline_id=f"pipe-{i:03d}",
                    pipeline_title=f"Pipeline {i}",
                    team_name="team_alpha",
                    source_file_path=f"/data/pipe-{i:03d}.json",
                ),
                "team_alpha",
            )

        sm.update_pipeline("pipe-000", status=PipelineStatus.APPROVED)
        sm.update_pipeline("pipe-001", status=PipelineStatus.APPROVED)
        sm.update_pipeline("pipe-002", status=PipelineStatus.IN_REVIEW)
        sm.update_pipeline("pipe-003", status=PipelineStatus.FAILED)

        state = sm.load()
        progress = state.get_team_progress("team_alpha")
        assert progress.total == 5
        assert progress.approved == 2
        assert progress.in_review == 1
        assert progress.failed == 1
        assert progress.pending == 1
        assert progress.completion_pct == 40.0

    def test_completion_pct_zero_for_empty_team(self, state_file):
        sm = StateManager(state_file)
        sm.save(sm.load())  # ensure file exists
        state = sm.load()
        progress = state.get_team_progress("nonexistent_team")
        assert progress.total == 0
        assert progress.completion_pct == 0.0
