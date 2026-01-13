from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from ..contracts import ArtifactStore
from ..errors import NotFoundError, ValidationError
from ...secretstore.loader import load_secretstore, env_for_integration


@dataclass(frozen=True)
class S3ArtifactStoreSettings:
    """Settings for S3ArtifactStore.

    Credentials are resolved via boto3's standard credential chain.

    Environment fallbacks (used when the corresponding setting field is empty):
      - bucket: PLATFORM_ARTIFACTS_S3_BUCKET then AWS_S3_BUCKET
      - region: AWS_REGION then AWS_DEFAULT_REGION

    prefix is optional and will be prepended to all keys.
    """

    bucket: str = ""
    prefix: str = ""
    region: str = ""


class S3ArtifactStore(ArtifactStore):
    """ArtifactStore backed by AWS S3."""

    def __init__(self, *, settings: S3ArtifactStoreSettings):
        self.settings = settings

    def _integration_env(self) -> dict:
        """Load integration settings from secretstore if available.

        This is a best-effort helper intended for local runs and CI where the
        repository's encrypted secretstore.json.gpg is present and
        SECRETSTORE_PASSPHRASE is provided.
        """
        # Try a small upward search to find the repo root.
        start = Path.cwd().resolve()
        candidates = [start] + list(start.parents)[:4]
        for base in candidates:
            gpg_path = base / "platform" / "secretstore" / "secretstore.json.gpg"
            if gpg_path.exists():
                store = load_secretstore(base)
                return env_for_integration(store, "artifact_store_s3")
        return {}

    def _bucket(self) -> str:
        b = str(self.settings.bucket or "").strip()
        if b:
            return b
        b = str(os.environ.get("PLATFORM_ARTIFACTS_S3_BUCKET", "") or "").strip()
        if b:
            return b
        integ = self._integration_env()
        b = str(integ.get("PLATFORM_ARTIFACTS_S3_BUCKET", "") or integ.get("AWS_S3_BUCKET", "") or "").strip()
        if b:
            return b
        b = str(os.environ.get("AWS_S3_BUCKET", "") or "").strip()
        if b:
            return b
        raise ValidationError("S3ArtifactStore bucket missing. Set artifact_store.settings.bucket or PLATFORM_ARTIFACTS_S3_BUCKET.")

    def _prefix(self) -> str:
        p = str(self.settings.prefix or "").strip().lstrip("/")
        if p and not p.endswith("/"):
            p = p + "/"
        return p

    def _client(self):
        import boto3

        integ = self._integration_env()

        region = str(self.settings.region or "").strip()
        if not region:
            region = str(os.environ.get("AWS_REGION", "") or os.environ.get("AWS_DEFAULT_REGION", "") or "").strip()
        if not region:
            region = str(integ.get("AWS_REGION", "") or integ.get("AWS_DEFAULT_REGION", "") or "").strip()

        ak = str(os.environ.get("AWS_ACCESS_KEY_ID", "") or "").strip() or str(integ.get("AWS_ACCESS_KEY_ID", "") or "").strip()
        sk = str(os.environ.get("AWS_SECRET_ACCESS_KEY", "") or "").strip() or str(integ.get("AWS_SECRET_ACCESS_KEY", "") or "").strip()
        st = str(os.environ.get("AWS_SESSION_TOKEN", "") or "").strip() or str(integ.get("AWS_SESSION_TOKEN", "") or "").strip()

        if ak and sk:
            sess_kwargs = {"aws_access_key_id": ak, "aws_secret_access_key": sk}
            if st:
                sess_kwargs["aws_session_token"] = st
            if region:
                sess_kwargs["region_name"] = region
            session = boto3.session.Session(**sess_kwargs)
            return session.client("s3")
        if region:
            return boto3.client("s3", region_name=region)
        return boto3.client("s3")

    def _normalize_key(self, key: str) -> str:
        k = str(key or "").lstrip("/")
        if not k:
            raise ValidationError("Artifact key must be non-empty")
        return f"{self._prefix()}{k}"

    def put_file(self, key: str, local_path: Path, content_type: str = "") -> str:
        from botocore.exceptions import ClientError

        bucket = self._bucket()
        k = self._normalize_key(key)
        p = Path(local_path)
        if not p.exists():
            raise NotFoundError(f"Local file not found for upload: {p}")

        extra: dict = {}
        ct = str(content_type or "").strip()
        if ct:
            extra["ContentType"] = ct

        client = self._client()
        try:
            if extra:
                client.upload_file(str(p), bucket, k, ExtraArgs=extra)
            else:
                client.upload_file(str(p), bucket, k)
        except ClientError as e:
            raise ValidationError(f"S3 upload failed: s3://{bucket}/{k} ({e})")

        return f"s3://{bucket}/{k}"

    def get_to_file(self, key: str, dest_path: Path) -> None:
        from botocore.exceptions import ClientError

        bucket = self._bucket()
        k = self._normalize_key(key)
        dest = Path(dest_path)
        dest.parent.mkdir(parents=True, exist_ok=True)

        client = self._client()
        try:
            client.download_file(bucket, k, str(dest))
        except ClientError as e:
            raise NotFoundError(f"S3 download failed: s3://{bucket}/{k} ({e})")

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        bucket = self._bucket()
        k = self._normalize_key(key)
        client = self._client()
        try:
            client.head_object(Bucket=bucket, Key=k)
            return True
        except ClientError as e:
            code = str(e.response.get("Error", {}).get("Code", ""))
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            return False

    def list_keys(self, prefix: str = "") -> List[str]:
        from botocore.exceptions import ClientError

        bucket = self._bucket()
        base = self._prefix()
        pref = str(prefix or "").lstrip("/")
        full_pref = f"{base}{pref}"

        client = self._client()
        paginator = client.get_paginator("list_objects_v2")
        out: List[str] = []
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=full_pref):
                for obj in page.get("Contents", []) or []:
                    k = str(obj.get("Key", "") or "")
                    if not k:
                        continue
                    if base and k.startswith(base):
                        k = k[len(base) :]
                    out.append(k)
        except ClientError as e:
            raise ValidationError(f"S3 list failed: s3://{bucket}/{full_pref} ({e})")

        return sorted(out)
