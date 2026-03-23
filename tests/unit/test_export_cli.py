"""
Tests for the migrate export CLI command.

Verifies that the CLI itself enforces the APPROVED-only filter — not just
the state manager logic, but the actual command invocation via Typer's
test runner.
"""
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cli.main import app
from models.migration import (
    DatabricksTargetFormat,
    GeneratedArtifact,
    PipelineRecord,
    PipelineStatus,
)
from state.state_manager import StateManager

runner = CliRunner()


def _write_artifact(output_dir: Path, team: str, pipeline_id: str, code: str):
    """Write a fake artifact to disk as migrate run would."""
    dest = output_dir / team / pipeline_id
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "pipeline.py").write_text(code)
    (dest / "migration_metadata.json").write_text('{"confidence_score": 0.9}')


def _register(state_file: Path, pipeline_id: str, status: PipelineStatus, output_dir: Path):
    sm = StateManager(state_file)
    artifact = None
    if status == PipelineStatus.APPROVED:
        artifact = GeneratedArtifact(
            artifact_type=DatabricksTargetFormat.DLT,
            filename="pipeline.py",
            content=f"# code for {pipeline_id}",
        )
    record = PipelineRecord(
        pipeline_id=pipeline_id,
        pipeline_title=f"Pipeline {pipeline_id}",
        team_name="team_alpha",
        source_file_path=f"/data/{pipeline_id}.json",
        status=status,
        current_artifact=artifact,
    )
    sm.register_pipeline(record, "team_alpha")

    if status == PipelineStatus.APPROVED:
        _write_artifact(output_dir, "team_alpha", pipeline_id, f"# code for {pipeline_id}")


class TestExportCLIApprovedOnly:
    @pytest.fixture(autouse=True)
    def patch_settings(self, mocker, tmp_path):
        """Point settings at tmp dirs so CLI uses isolated state + output."""
        state_file = tmp_path / "state" / "migration_state.json"
        output_dir = tmp_path / "output"
        output_dir.mkdir(parents=True)

        mocker.patch("cli.commands.export.settings")
        import cli.commands.export as exp_mod
        exp_mod.settings.state_file = state_file
        exp_mod.settings.output_dir = output_dir

        self.state_file = state_file
        self.output_dir = output_dir

    def test_only_approved_pipeline_is_exported(self, tmp_path):
        _register(self.state_file, "pipe-approved", PipelineStatus.APPROVED, self.output_dir)
        _register(self.state_file, "pipe-pending", PipelineStatus.PENDING, self.output_dir)
        _register(self.state_file, "pipe-in-review", PipelineStatus.IN_REVIEW, self.output_dir)
        _register(self.state_file, "pipe-failed", PipelineStatus.FAILED, self.output_dir)

        export_dir = tmp_path / "deploy"
        result = runner.invoke(app, ["export", str(export_dir)])

        assert result.exit_code == 0, result.output
        assert "1 pipelines" in result.output

        # Only the approved pipeline's files should be present
        assert (export_dir / "team_alpha" / "pipe-approved" / "pipeline.py").exists()
        assert not (export_dir / "team_alpha" / "pipe-pending").exists()
        assert not (export_dir / "team_alpha" / "pipe-in-review").exists()
        assert not (export_dir / "team_alpha" / "pipe-failed").exists()

    def test_no_approved_pipelines_exits_gracefully(self, tmp_path):
        _register(self.state_file, "pipe-pending", PipelineStatus.PENDING, self.output_dir)
        _register(self.state_file, "pipe-failed", PipelineStatus.FAILED, self.output_dir)

        export_dir = tmp_path / "deploy"
        result = runner.invoke(app, ["export", str(export_dir)])

        assert result.exit_code == 0
        assert "No approved" in result.output
        assert not export_dir.exists()

    def test_all_approved_pipelines_exported(self, tmp_path):
        _register(self.state_file, "pipe-001", PipelineStatus.APPROVED, self.output_dir)
        _register(self.state_file, "pipe-002", PipelineStatus.APPROVED, self.output_dir)
        _register(self.state_file, "pipe-003", PipelineStatus.APPROVED, self.output_dir)

        export_dir = tmp_path / "deploy"
        result = runner.invoke(app, ["export", str(export_dir)])

        assert result.exit_code == 0
        assert "3 pipelines" in result.output
        for pid in ["pipe-001", "pipe-002", "pipe-003"]:
            assert (export_dir / "team_alpha" / pid / "pipeline.py").exists()

    def test_export_to_same_dir_as_output_fails_with_clear_error(self):
        """Exporting to OUTPUT_DIR itself must fail clearly, not crash with SameFileError."""
        _register(self.state_file, "pipe-approved", PipelineStatus.APPROVED, self.output_dir)

        result = runner.invoke(app, ["export", str(self.output_dir)])

        assert result.exit_code == 1
        assert "cannot be inside the output directory" in result.output

    def test_export_to_subdir_of_output_fails(self):
        """A subdirectory of OUTPUT_DIR is also rejected."""
        _register(self.state_file, "pipe-approved", PipelineStatus.APPROVED, self.output_dir)

        subdir = self.output_dir / "deploy"
        result = runner.invoke(app, ["export", str(subdir)])

        assert result.exit_code == 1
        assert "cannot be inside the output directory" in result.output

    def test_export_zip_excludes_non_approved(self, tmp_path):
        _register(self.state_file, "pipe-approved", PipelineStatus.APPROVED, self.output_dir)
        _register(self.state_file, "pipe-pending", PipelineStatus.PENDING, self.output_dir)

        zip_path = tmp_path / "bundle.zip"
        result = runner.invoke(app, ["export", str(zip_path), "--format", "zip"])

        assert result.exit_code == 0
        import zipfile
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
        assert any("pipe-approved" in n for n in names)
        assert not any("pipe-pending" in n for n in names)
