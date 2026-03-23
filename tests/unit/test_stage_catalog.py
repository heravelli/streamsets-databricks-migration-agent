"""Unit tests for catalog/stage_catalog.py"""
import pytest
import yaml
from pathlib import Path

from catalog.stage_catalog import StageCatalog
from models.stage_mapping import ConfidenceLevel, DatabricksTargetType


# ── Helpers ──────────────────────────────────────────────────────────────────

def _write_catalog(tmp_path: Path, filename: str, stages: list[dict]) -> Path:
    path = tmp_path / filename
    path.write_text(yaml.dump({"stages": stages}))
    return path


KAFKA_STAGE = {
    "streamsets_stage": "com.streamsets.pipeline.stage.origin.kafka.KafkaDSource",
    "streamsets_label": "Kafka Consumer",
    "databricks_equivalent": "kafka_streaming",
    "databricks_label": "Kafka Streaming",
    "confidence": "exact",
    "code_template": "spark.readStream.format('kafka')...",
    "config_mapping": {"kafkaConfigBean.metadataBrokerList": "bootstrap_servers"},
    "notes": "Use DLT for streaming",
    "requires_manual_review": False,
}

JDBC_TARGET_STAGE = {
    "streamsets_stage": "com.streamsets.pipeline.stage.destination.jdbc.JdbcDTarget",
    "streamsets_label": "JDBC Producer",
    "databricks_equivalent": "delta_merge",
    "databricks_label": "Delta Lake Write",
    "confidence": "high",
    "code_template": "df.write.format('delta')...",
    "config_mapping": {},
    "notes": "",
    "requires_manual_review": False,
}

UNSUPPORTED_STAGE = {
    "streamsets_stage": "com.streamsets.pipeline.stage.legacy.OldDSource",
    "streamsets_label": "Old Stage",
    "databricks_equivalent": "custom_python",
    "databricks_label": "Custom Python",
    "confidence": "unsupported",
    "code_template": "",
    "config_mapping": {},
    "notes": "No equivalent",
    "requires_manual_review": True,
}


# ── StageCatalog loading ─────────────────────────────────────────────────────

class TestStageCatalogLoading:
    def test_loads_stages_from_yaml(self, tmp_path):
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE])
        catalog = StageCatalog(tmp_path)
        assert len(catalog.all_stage_names()) == 1

    def test_loads_multiple_yaml_files(self, tmp_path):
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE])
        _write_catalog(tmp_path, "destinations.yaml", [JDBC_TARGET_STAGE])
        catalog = StageCatalog(tmp_path)
        assert len(catalog.all_stage_names()) == 2

    def test_empty_directory_loads_empty_catalog(self, tmp_path):
        catalog = StageCatalog(tmp_path)
        assert catalog.all_stage_names() == []

    def test_real_catalog_has_78_stages(self, catalog_dir):
        catalog = StageCatalog(catalog_dir)
        count = len(catalog.all_stage_names())
        # Allow a small variance in case catalog was updated, but expect ~78 entries
        assert 70 <= count <= 90, f"Expected ~78 stages, got {count}"


# ── StageCatalog lookup ──────────────────────────────────────────────────────

class TestStageCatalogLookup:
    def test_exact_lookup_by_fqcn(self, tmp_path):
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE])
        catalog = StageCatalog(tmp_path)
        mapping = catalog.lookup("com.streamsets.pipeline.stage.origin.kafka.KafkaDSource")
        assert mapping is not None
        assert mapping.streamsets_label == "Kafka Consumer"

    def test_lookup_returns_none_for_unknown_stage(self, tmp_path):
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE])
        catalog = StageCatalog(tmp_path)
        assert catalog.lookup("com.totally.unknown.StageDSource") is None

    def test_suffix_fallback_lookup(self, tmp_path):
        """Catalog should match by class name suffix when FQCN doesn't match exactly."""
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE])
        catalog = StageCatalog(tmp_path)
        # Different package path, same class name suffix
        mapping = catalog.lookup("com.alt.package.kafka.KafkaDSource")
        assert mapping is not None

    def test_confidence_level_correct(self, tmp_path):
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE])
        catalog = StageCatalog(tmp_path)
        mapping = catalog.lookup("com.streamsets.pipeline.stage.origin.kafka.KafkaDSource")
        assert mapping.confidence == ConfidenceLevel.EXACT

    def test_requires_manual_review_flag(self, tmp_path):
        _write_catalog(tmp_path, "legacy.yaml", [UNSUPPORTED_STAGE])
        catalog = StageCatalog(tmp_path)
        mapping = catalog.lookup("com.streamsets.pipeline.stage.legacy.OldDSource")
        assert mapping.requires_manual_review is True


# ── lookup_all and get_unmapped ──────────────────────────────────────────────

class TestLookupAllAndUnmapped:
    def test_lookup_all_returns_dict(self, tmp_path):
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE, JDBC_TARGET_STAGE])
        catalog = StageCatalog(tmp_path)
        names = [
            "com.streamsets.pipeline.stage.origin.kafka.KafkaDSource",
            "com.streamsets.pipeline.stage.destination.jdbc.JdbcDTarget",
            "com.unknown.Stage",
        ]
        result = catalog.lookup_all(names)
        assert len(result) == 3
        assert result["com.unknown.Stage"] is None

    def test_get_unmapped_returns_missing(self, tmp_path):
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE])
        catalog = StageCatalog(tmp_path)
        unmapped = catalog.get_unmapped([
            "com.streamsets.pipeline.stage.origin.kafka.KafkaDSource",
            "com.no.mapping.Here",
        ])
        assert unmapped == ["com.no.mapping.Here"]

    def test_get_unmapped_returns_empty_when_all_mapped(self, tmp_path):
        _write_catalog(tmp_path, "origins.yaml", [KAFKA_STAGE])
        catalog = StageCatalog(tmp_path)
        unmapped = catalog.get_unmapped(["com.streamsets.pipeline.stage.origin.kafka.KafkaDSource"])
        assert unmapped == []


# ── coverage_report ──────────────────────────────────────────────────────────

class TestCoverageReport:
    def test_coverage_report_total(self, tmp_path):
        _write_catalog(tmp_path, "stages.yaml", [KAFKA_STAGE, JDBC_TARGET_STAGE, UNSUPPORTED_STAGE])
        catalog = StageCatalog(tmp_path)
        report = catalog.coverage_report()
        assert report["total_mapped"] == 3

    def test_coverage_report_by_confidence(self, tmp_path):
        _write_catalog(tmp_path, "stages.yaml", [KAFKA_STAGE, JDBC_TARGET_STAGE, UNSUPPORTED_STAGE])
        catalog = StageCatalog(tmp_path)
        report = catalog.coverage_report()
        assert report["by_confidence"]["exact"] == 1
        assert report["by_confidence"]["high"] == 1
        assert report["by_confidence"]["unsupported"] == 1

    def test_real_catalog_coverage_report(self, catalog_dir):
        catalog = StageCatalog(catalog_dir)
        report = catalog.coverage_report()
        assert report["total_mapped"] > 0
        assert "exact" in report["by_confidence"]
