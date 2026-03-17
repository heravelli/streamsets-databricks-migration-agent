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
        if stage_name in self._mappings:
            return self._mappings[stage_name]
        # Suffix match for versioned or alternate class names
        stage_short = stage_name.split(".")[-1].lower()
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
