from pathlib import Path
import yaml
from models.stage_mapping import StageMapping, ConfidenceLevel


class StageCatalog:
    def __init__(self, catalog_dir: Path):
        self._mappings: dict[str, StageMapping] = {}
        self._load_all(catalog_dir)

    def _load_all(self, catalog_dir: Path) -> None:
        for yaml_file in sorted(catalog_dir.glob("*.yaml")):
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
            for entry in data.get("stages", []):
                mapping = StageMapping(**entry)
                self._mappings[mapping.streamsets_stage] = mapping

    def lookup(self, stage_name: str) -> StageMapping | None:
        # 1. Exact match (dot notation)
        if stage_name in self._mappings:
            return self._mappings[stage_name]
        # 2. Normalize underscore-separated FQCNs → dot notation.
        #    Real SDC pipeline exports use underscores as package separators, e.g.:
        #      com_streamsets_pipeline_stage_origin_multikafka_MultiKafkaDSource
        #    Our catalog stores the canonical Java dot notation:
        #      com.streamsets.pipeline.stage.origin.multikafka.MultiKafkaDSource
        if "_" in stage_name and "." not in stage_name:
            normalized = stage_name.replace("_", ".")
            if normalized in self._mappings:
                return self._mappings[normalized]
            stage_short = normalized.split(".")[-1].lower()
        else:
            stage_short = stage_name.split(".")[-1].lower()
        # 3. Suffix match — handles versioned class names and minor path variants
        for key, mapping in self._mappings.items():
            if key.split(".")[-1].lower() == stage_short:
                return mapping
        return None

    def lookup_all(self, stage_names: list[str]) -> dict[str, StageMapping | None]:
        return {name: self.lookup(name) for name in stage_names}

    def get_unmapped(self, stage_names: list[str]) -> list[str]:
        return [name for name in stage_names if self.lookup(name) is None]

    def coverage_report(self) -> dict:
        return {
            "total_mapped": len(self._mappings),
            "by_confidence": {
                level.value: sum(1 for m in self._mappings.values() if m.confidence == level)
                for level in ConfidenceLevel
            },
        }

    def all_stage_names(self) -> list[str]:
        return list(self._mappings.keys())
