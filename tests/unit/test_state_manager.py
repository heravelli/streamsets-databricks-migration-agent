"""Unit tests for state/state_manager.py"""
import pytest
from pathlib import Path

from state.state_manager import StateManager
from models.migration import PipelineRecord, PipelineStatus, MigrationState


def _make_record(pipeline_id: str, team: str = "team_alpha") -> PipelineRecord:
    return PipelineRecord(
        pipeline_id=pipeline_id,
        pipeline_title=f"Pipeline {pipeline_id}",
        team_name=team,
        source_file_path=f"/data/{team}/{pipeline_id}.json",
    )


class TestStateManagerLoad:
    def test_load_returns_empty_state_when_no_file(self, state_file):
        sm = StateManager(state_file)
        state = sm.load()
        assert isinstance(state, MigrationState)
        assert state.pipelines == {}
        assert state.team_index == {}

    def test_load_reads_existing_state(self, state_file):
        sm = StateManager(state_file)
        record = _make_record("pipe-001")
        sm.register_pipeline(record, "team_alpha")

        sm2 = StateManager(state_file)
        state = sm2.load()
        assert "pipe-001" in state.pipelines

    def test_creates_parent_directory(self, tmp_path):
        deep_path = tmp_path / "a" / "b" / "c" / "state.json"
        sm = StateManager(deep_path)
        assert deep_path.parent.exists()


class TestStateManagerSave:
    def test_save_creates_file(self, state_file):
        sm = StateManager(state_file)
        sm.save(MigrationState())
        assert state_file.exists()

    def test_save_is_atomic(self, state_file):
        """Verify no .tmp file is left behind after save."""
        sm = StateManager(state_file)
        sm.save(MigrationState())
        assert not state_file.with_suffix(".tmp").exists()

    def test_save_updates_last_updated(self, state_file):
        sm = StateManager(state_file)
        state = sm.load()
        before = state.last_updated
        sm.save(state)
        reloaded = sm.load()
        assert reloaded.last_updated >= before


class TestRegisterPipeline:
    def test_register_adds_to_pipelines(self, state_file):
        sm = StateManager(state_file)
        record = _make_record("pipe-001")
        sm.register_pipeline(record, "team_alpha")

        state = sm.load()
        assert "pipe-001" in state.pipelines

    def test_register_adds_to_team_index(self, state_file):
        sm = StateManager(state_file)
        record = _make_record("pipe-001")
        sm.register_pipeline(record, "team_alpha")

        state = sm.load()
        assert "pipe-001" in state.team_index["team_alpha"]

    def test_register_multiple_pipelines_same_team(self, state_file):
        sm = StateManager(state_file)
        sm.register_pipeline(_make_record("pipe-001"), "team_alpha")
        sm.register_pipeline(_make_record("pipe-002"), "team_alpha")

        state = sm.load()
        assert len(state.team_index["team_alpha"]) == 2

    def test_register_idempotent(self, state_file):
        sm = StateManager(state_file)
        record = _make_record("pipe-001")
        sm.register_pipeline(record, "team_alpha")
        sm.register_pipeline(record, "team_alpha")  # duplicate

        state = sm.load()
        assert state.team_index["team_alpha"].count("pipe-001") == 1

    def test_register_multiple_teams(self, state_file):
        sm = StateManager(state_file)
        sm.register_pipeline(_make_record("pipe-001", "team_a"), "team_a")
        sm.register_pipeline(_make_record("pipe-002", "team_b"), "team_b")

        state = sm.load()
        assert "team_a" in state.team_index
        assert "team_b" in state.team_index


class TestGetPipeline:
    def test_get_existing_pipeline(self, state_file):
        sm = StateManager(state_file)
        record = _make_record("pipe-001")
        sm.register_pipeline(record, "team_alpha")

        fetched = sm.get_pipeline("pipe-001")
        assert fetched is not None
        assert fetched.pipeline_id == "pipe-001"

    def test_get_missing_pipeline_returns_none(self, state_file):
        sm = StateManager(state_file)
        assert sm.get_pipeline("does-not-exist") is None


class TestUpdatePipeline:
    def test_update_status(self, state_file):
        sm = StateManager(state_file)
        sm.register_pipeline(_make_record("pipe-001"), "team_alpha")
        sm.update_pipeline("pipe-001", status=PipelineStatus.IN_REVIEW)

        record = sm.get_pipeline("pipe-001")
        assert record.status == PipelineStatus.IN_REVIEW

    def test_update_nonexistent_raises_key_error(self, state_file):
        sm = StateManager(state_file)
        with pytest.raises(KeyError):
            sm.update_pipeline("ghost-pipeline", status=PipelineStatus.FAILED)

    def test_update_multiple_fields(self, state_file):
        sm = StateManager(state_file)
        sm.register_pipeline(_make_record("pipe-001"), "team_alpha")
        sm.update_pipeline("pipe-001", status=PipelineStatus.APPROVED, migration_attempts=3)

        record = sm.get_pipeline("pipe-001")
        assert record.status == PipelineStatus.APPROVED
        assert record.migration_attempts == 3


class TestGetPipelinesByStatus:
    def test_filter_by_status(self, state_file):
        sm = StateManager(state_file)
        sm.register_pipeline(_make_record("pipe-001"), "team_alpha")
        sm.register_pipeline(_make_record("pipe-002"), "team_alpha")
        sm.update_pipeline("pipe-001", status=PipelineStatus.APPROVED)

        approved = sm.get_pipelines_by_status(PipelineStatus.APPROVED)
        assert len(approved) == 1
        assert approved[0].pipeline_id == "pipe-001"

    def test_filter_by_status_and_team(self, state_file):
        sm = StateManager(state_file)
        sm.register_pipeline(_make_record("pipe-001", "team_a"), "team_a")
        sm.register_pipeline(_make_record("pipe-002", "team_b"), "team_b")
        sm.update_pipeline("pipe-001", status=PipelineStatus.APPROVED)
        sm.update_pipeline("pipe-002", status=PipelineStatus.APPROVED)

        approved_a = sm.get_pipelines_by_status(PipelineStatus.APPROVED, "team_a")
        assert len(approved_a) == 1
        assert approved_a[0].pipeline_id == "pipe-001"

    def test_get_pending_for_team(self, state_file):
        sm = StateManager(state_file)
        sm.register_pipeline(_make_record("pipe-001"), "team_alpha")
        sm.register_pipeline(_make_record("pipe-002"), "team_alpha")
        sm.update_pipeline("pipe-001", status=PipelineStatus.APPROVED)

        pending = sm.get_pending_for_team("team_alpha")
        assert len(pending) == 1
        assert pending[0].pipeline_id == "pipe-002"


class TestBackup:
    def test_backup_creates_file(self, state_file):
        sm = StateManager(state_file)
        sm.register_pipeline(_make_record("pipe-001"), "team_alpha")
        backup_path = sm.backup()
        assert backup_path.exists()

    def test_backup_in_backups_subdir(self, state_file):
        sm = StateManager(state_file)
        sm.save(MigrationState())
        backup_path = sm.backup()
        assert backup_path.parent.name == "backups"
