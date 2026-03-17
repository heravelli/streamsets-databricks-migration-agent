import json
from datetime import datetime
from pathlib import Path

import portalocker

from models.migration import MigrationState, PipelineRecord, PipelineStatus


class StateManager:
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> MigrationState:
        if not self.state_file.exists():
            return MigrationState()
        with open(self.state_file) as f:
            return MigrationState.model_validate_json(f.read())

    def save(self, state: MigrationState) -> None:
        """Atomic write with cross-platform file locking (portalocker)."""
        state.last_updated = datetime.utcnow()
        lock_file = self.state_file.with_suffix(".lock")
        lock_file.touch()

        with portalocker.Lock(str(lock_file), timeout=30):
            tmp = self.state_file.with_suffix(".tmp")
            tmp.write_text(state.model_dump_json(indent=2))
            tmp.replace(self.state_file)

    def register_pipeline(
        self, record: PipelineRecord, team_name: str
    ) -> PipelineRecord:
        state = self.load()
        state.pipelines[record.pipeline_id] = record
        if team_name not in state.team_index:
            state.team_index[team_name] = []
        if record.pipeline_id not in state.team_index[team_name]:
            state.team_index[team_name].append(record.pipeline_id)
        self.save(state)
        return record

    def get_pipeline(self, pipeline_id: str) -> PipelineRecord | None:
        state = self.load()
        return state.pipelines.get(pipeline_id)

    def update_pipeline(self, pipeline_id: str, **kwargs) -> PipelineRecord:
        state = self.load()
        if pipeline_id not in state.pipelines:
            raise KeyError(f"Pipeline {pipeline_id} not found in state")
        record = state.pipelines[pipeline_id]
        for key, value in kwargs.items():
            setattr(record, key, value)
        record.updated_at = datetime.utcnow()
        state.pipelines[pipeline_id] = record
        self.save(state)
        return record

    def get_pipelines_by_status(
        self, status: PipelineStatus, team_name: str | None = None
    ) -> list[PipelineRecord]:
        state = self.load()
        records = list(state.pipelines.values())
        if team_name:
            ids = set(state.team_index.get(team_name, []))
            records = [r for r in records if r.pipeline_id in ids]
        return [r for r in records if r.status == status]

    def get_pending_for_team(self, team_name: str) -> list[PipelineRecord]:
        return self.get_pipelines_by_status(PipelineStatus.PENDING, team_name)

    def backup(self) -> Path:
        state = self.load()
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.state_file.parent / "backups"
        backup_dir.mkdir(exist_ok=True)
        backup_path = backup_dir / f"migration_state_{ts}.json"
        backup_path.write_text(state.model_dump_json(indent=2))
        return backup_path
