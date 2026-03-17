from enum import Enum
from pydantic import BaseModel


class ConfidenceLevel(str, Enum):
    EXACT = "exact"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNSUPPORTED = "unsupported"


class DatabricksTargetType(str, Enum):
    DLT_TABLE = "dlt_table"
    DLT_STREAMING_TABLE = "dlt_streaming_table"
    SPARK_DATAFRAME = "spark_dataframe"
    SPARK_SQL = "spark_sql"
    CUSTOM_PYTHON = "custom_python"
    DBUTILS = "dbutils"
    AUTOLOADER = "autoloader"
    DELTA_MERGE = "delta_merge"
    KAFKA_STREAMING = "kafka_streaming"


class StageMapping(BaseModel):
    streamsets_stage: str
    streamsets_label: str = ""
    databricks_equivalent: DatabricksTargetType = DatabricksTargetType.SPARK_DATAFRAME
    databricks_label: str = ""
    confidence: ConfidenceLevel = ConfidenceLevel.MEDIUM
    code_template: str = ""
    config_mapping: dict[str, str] = {}
    notes: str = ""
    requires_manual_review: bool = False
    documentation_url: str = ""
