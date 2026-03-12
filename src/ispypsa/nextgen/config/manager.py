from pathlib import Path
from typing import Any, Dict

import yaml
from pydantic import BaseModel


class DeepMergeConfigManager:
    """
    Hierarchical Configuration Manager.
    Merges default ISP datasets with custom user assumptions.
    """

    def __init__(self, default_config_path: Path):
        self.default_config = self._load_yaml(default_config_path)
        self.active_config = self.default_config.copy()

    def _load_yaml(self, path: Path) -> Dict[str, Any]:
        with open(path) as f:
            return yaml.safe_load(f) or {}

    def apply_override(self, override_path: Path) -> None:
        """
        Deep merge custom overrides into the active configuration.
        """
        overrides = self._load_yaml(override_path)
        self.active_config = self._deep_merge(self.active_config, overrides)

    def _deep_merge(self, base: dict, update: dict) -> dict:
        merged = base.copy()
        for k, v in update.items():
            if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
                merged[k] = self._deep_merge(merged[k], v)
            else:
                merged[k] = v
        return merged

    def get_validated_config(self, model_class: type[BaseModel]) -> BaseModel:
        """
        Return the strongly typed Pydantic representation of the active config.
        """
        return model_class(**self.active_config)
