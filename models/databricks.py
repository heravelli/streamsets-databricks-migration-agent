from typing import Optional
from pydantic import BaseModel


class DLTExpectation(BaseModel):
    name: str
    constraint: str
    action: str = "WARN"  # WARN, DROP, FAIL


class DLTTable(BaseModel):
    table_name: str
    is_streaming: bool = False
    source_format: str = ""
    transformation_sql: Optional[str] = None
    expectations: list[DLTExpectation] = []
    partitioned_by: list[str] = []
    comment: str = ""


class DatabricksJobTask(BaseModel):
    task_key: str
    notebook_path: Optional[str] = None
    python_file: Optional[str] = None
    depends_on: list[str] = []
    parameters: dict = {}


class DatabricksJobDefinition(BaseModel):
    job_name: str
    tasks: list[DatabricksJobTask] = []
    cluster_config: dict = {}
    schedule: Optional[str] = None


class MigrationArtifact(BaseModel):
    pipeline_id: str
    pipeline_title: str
    target_format: str
    python_code: str
    notebook_json: Optional[dict] = None
    job_definition: Optional[dict] = None
    metadata: dict = {}
