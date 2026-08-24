"""Runtime settings shared by lakehouse pipeline stages."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PipelineSettings:
    """Filesystem locations and execution settings for a local pipeline run."""

    data_root: Path = Path("data")
    app_name: str = "housing-data-lakehouse"

    @property
    def bronze_path(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_path(self) -> Path:
        return self.data_root / "gold"

    def create_data_directories(self) -> None:
        """Create medallion-layer directories when they do not exist."""
        for path in (self.bronze_path, self.silver_path, self.gold_path):
            path.mkdir(parents=True, exist_ok=True)
