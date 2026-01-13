from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from ..contracts import ArtifactStore
from ..errors import ValidationError


@dataclass(frozen=True)
class MultiArtifactStoreSettings:
    policy: str = "fail_fast"


class MultiArtifactStore(ArtifactStore):
    def __init__(self, stores: List[ArtifactStore], settings: MultiArtifactStoreSettings):
        if not stores:
            raise ValidationError("MultiArtifactStore requires at least one child store")
        self.stores = list(stores)
        self.settings = settings
        pol = str(settings.policy or "").strip().lower()
        if pol not in ("fail_fast", "best_effort_secondary"):
            raise ValidationError(f"Unknown MultiArtifactStore policy: {settings.policy!r}")

    def put_file(self, key: str, local_path: Path, content_type: str = "") -> str:
        pol = str(self.settings.policy or "").strip().lower()
        primary_uri = ""
        first_error: Exception | None = None
        for idx, store in enumerate(self.stores):
            try:
                uri = store.put_file(key, local_path, content_type=content_type)
                if idx == 0:
                    primary_uri = uri
            except Exception as e:
                if first_error is None:
                    first_error = e
                if pol == "fail_fast" or idx == 0:
                    raise
        if not primary_uri:
            raise ValidationError("MultiArtifactStore primary store did not return a URI")
        if first_error is not None and pol == "best_effort_secondary":
            return primary_uri
        return primary_uri

    def get_to_file(self, key: str, dest_path: Path) -> None:
        self.stores[0].get_to_file(key, dest_path)

    def exists(self, key: str) -> bool:
        return self.stores[0].exists(key)

    def list_keys(self, prefix: str = "") -> List[str]:
        return self.stores[0].list_keys(prefix)
