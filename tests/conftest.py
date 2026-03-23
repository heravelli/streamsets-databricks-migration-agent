"""Shared fixtures used across unit and integration tests."""
import json
import pytest
from pathlib import Path


# ── Sample pipeline dicts (mirrors the real files in data/pipelines/) ─────────

KAFKA_PIPELINE_DICT = {
    "pipelineId": "pipeline-kafka-001",
    "title": "Kafka Orders to Delta Lake",
    "executionMode": "CONTINUOUS",
    "stages": [
        {
            "instanceName": "KafkaConsumer_01",
            "stageName": "com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource",
            "library": "streamsets-datacollector-kafka-lib",
            "type": "SOURCE",
            "configuration": [
                {"name": "conf.brokerURI", "value": "kafka:9092"},
                {"name": "conf.topicList", "value": ["orders"]},
                {"name": "conf.consumerGroup", "value": "sdc-group"},
                {"name": "conf.kafkaAutoOffsetReset", "value": "EARLIEST"},
            ],
            "inputLanes": [],
            "outputLanes": ["KafkaConsumer_01OutputLane0"],
            "eventLanes": [],
            "uiInfo": {},
        },
        {
            "instanceName": "FieldTypeConverter_01",
            "stageName": "com.streamsets.pipeline.stage.processor.fieldtypeconverter.FieldTypeConverterDProcessor",
            "library": "streamsets-datacollector-basic-lib",
            "type": "PROCESSOR",
            "configuration": [
                {"name": "conversions", "value": [{"fields": ["/order_id"], "targetType": "LONG"}]},
            ],
            "inputLanes": ["KafkaConsumer_01OutputLane0"],
            "outputLanes": ["FieldTypeConverter_01OutputLane0"],
            "eventLanes": [],
            "uiInfo": {},
        },
        {
            "instanceName": "HiveMetastore_01",
            "stageName": "com.streamsets.pipeline.stage.destination.jdbc.JdbcDTarget",
            "library": "streamsets-datacollector-jdbc-lib",
            "type": "TARGET",
            "configuration": [
                {"name": "hikariConfigBean.connectionString", "value": "jdbc:hive2://hive:10000"},
                {"name": "tableNameTemplate", "value": "raw_orders"},
            ],
            "inputLanes": ["FieldTypeConverter_01OutputLane0"],
            "outputLanes": [],
            "eventLanes": [],
            "uiInfo": {},
        },
    ],
    "errorStage": None,
    "parameters": {},
}

GROOVY_PIPELINE_DICT = {
    "pipelineId": "pipeline-groovy-001",
    "title": "Groovy Transform Pipeline",
    "executionMode": "STANDALONE",
    "stages": [
        {
            "instanceName": "JdbcOrigin_01",
            "stageName": "com.streamsets.pipeline.stage.origin.jdbc.JdbcDSource",
            "library": "streamsets-datacollector-jdbc-lib",
            "type": "SOURCE",
            "configuration": [
                {"name": "hikariConfigBean.connectionString", "value": "jdbc:postgresql://db:5432/app"},
                {"name": "tableJdbcConfigBean.schema", "value": "public"},
            ],
            "inputLanes": [],
            "outputLanes": ["JdbcOrigin_01OutputLane0"],
            "eventLanes": [],
            "uiInfo": {},
        },
        {
            "instanceName": "GroovyEval_01",
            "stageName": "com.streamsets.pipeline.stage.processor.groovy.GroovyDProcessor",
            "library": "streamsets-datacollector-groovy-lib",
            "type": "PROCESSOR",
            "configuration": [
                {"name": "groovyScript", "value": "for (record in records) { record.value['/score'] = 42; output.write(record) }"},
            ],
            "inputLanes": ["JdbcOrigin_01OutputLane0"],
            "outputLanes": ["GroovyEval_01OutputLane0"],
            "eventLanes": [],
            "uiInfo": {},
        },
        {
            "instanceName": "JdbcTarget_01",
            "stageName": "com.streamsets.pipeline.stage.destination.jdbc.JdbcDTarget",
            "library": "streamsets-datacollector-jdbc-lib",
            "type": "TARGET",
            "configuration": [
                {"name": "hikariConfigBean.connectionString", "value": "jdbc:postgresql://db:5432/app"},
                {"name": "tableNameTemplate", "value": "scored_records"},
            ],
            "inputLanes": ["GroovyEval_01OutputLane0"],
            "outputLanes": [],
            "eventLanes": [],
            "uiInfo": {},
        },
    ],
    "errorStage": None,
    "parameters": {},
}


@pytest.fixture
def kafka_pipeline_dict():
    return KAFKA_PIPELINE_DICT.copy()


@pytest.fixture
def groovy_pipeline_dict():
    return GROOVY_PIPELINE_DICT.copy()


@pytest.fixture
def pipelines_dir(tmp_path):
    """A temp directory with sample pipeline JSON files."""
    team_dir = tmp_path / "team_alpha"
    team_dir.mkdir()
    (team_dir / "kafka_to_delta.json").write_text(json.dumps(KAFKA_PIPELINE_DICT))
    (team_dir / "groovy_pipeline.json").write_text(json.dumps(GROOVY_PIPELINE_DICT))
    return team_dir


@pytest.fixture
def catalog_dir():
    """Return the real catalog directory."""
    return Path(__file__).parent.parent / "catalog" / "stages"


@pytest.fixture
def state_file(tmp_path):
    """A temporary state file path."""
    return tmp_path / "state" / "migration_state.json"
