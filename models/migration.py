from datetime import datetime
from enum import Enum
from typing import Optional
import uuid
from pydantic import BaseModel, Field


class PipelineStatus(str, Enum):
    PENDING = "pending"
    CLASSIFYING = "classifying"
    MIGRATING = "migrating"
    IN_REVIEW = "in_review"
    CHANGES_REQUESTED = "changes_requested"
    RE_MIGRATING = "re_migrating"
    APPROVED = "approved"
    REJECTED = "rejected"
    ESCALATED = "escalated"
    FAILED = "failed"


class DatabricksTargetFormat(str, Enum):
    DLT = "dlt"
    JOB = "job"
    NOTEBOOK = "notebook"


class ReviewDecision(str, Enum):
    APPROVE = "approve"
    REQUEST_CHANGES = "request_changes"
    ESCALATE = "escalate"
    REJECT = "reject"


class ReviewComment(BaseModel):
    reviewer_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    decision: ReviewDecision
    feedback: str = ""
    line_references: list[int] = Field(default_factory=list)


class GeneratedArtifact(BaseModel):
    artifact_type: DatabricksTargetFormat
    filename: str
    content: str
    agent_reasoning: str = ""
    confidence_score: float = 0.0
    unmapped_stages: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class PipelineRecord(BaseModel):
    record_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str
    pipeline_title: str
    team_name: str
    source_file_path: str
    status: PipelineStatus = PipelineStatus.PENDING
    recommended_format: Optional[DatabricksTargetFormat] = None
    current_artifact: Optional[GeneratedArtifact] = None
    previous_artifact: Optional[GeneratedArtifact] = None
    review_history: list[ReviewComment] = Field(default_factory=list)
    agent_conversation: list[dict] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    migration_attempts: int = 0
    error_message: str = ""


class TeamProgress(BaseModel):
    team_name: str
    total: int = 0
    pending: int = 0
    in_review: int = 0
    approved: int = 0
    rejected: int = 0
    failed: int = 0
    escalated: int = 0
    completion_pct: float = 0.0


class MigrationState(BaseModel):
    project_name: str = "StreamSets to Databricks Migration"
    schema_version: str = "1.0"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    pipelines: dict[str, PipelineRecord] = Field(default_factory=dict)
    team_index: dict[str, list[str]] = Field(default_factory=dict)

    def get_team_progress(self, team_name: str) -> TeamProgress:
        pipeline_ids = self.team_index.get(team_name, [])
        records = [self.pipelines[pid] for pid in pipeline_ids if pid in self.pipelines]
        progress = TeamProgress(
            team_name=team_name,
            total=len(records),
            pending=sum(1 for r in records if r.status == PipelineStatus.PENDING),
            in_review=sum(
                1 for r in records
                if r.status in {PipelineStatus.IN_REVIEW, PipelineStatus.RE_MIGRATING,
                                PipelineStatus.CHANGES_REQUESTED}
            ),
            approved=sum(1 for r in records if r.status == PipelineStatus.APPROVED),
            rejected=sum(1 for r in records if r.status == PipelineStatus.REJECTED),
            escalated=sum(1 for r in records if r.status == PipelineStatus.ESCALATED),
            failed=sum(1 for r in records if r.status == PipelineStatus.FAILED),
        )
        if progress.total > 0:
            progress.completion_pct = round(progress.approved / progress.total * 100, 1)
        return progress

    def all_team_progress(self) -> list[TeamProgress]:
        return sorted(
            [self.get_team_progress(team) for team in self.team_index],
            key=lambda t: t.completion_pct,
        )
