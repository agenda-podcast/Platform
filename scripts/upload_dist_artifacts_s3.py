from __future__ import annotations

import sys
from pathlib import Path as _Path

# Ensure repo root is on sys.path so local 'platform' package wins over stdlib 'platform' module
_REPO_ROOT = _Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
if "platform" in sys.modules and not hasattr(sys.modules["platform"], "__path__"):
    del sys.modules["platform"]

import argparse
import mimetypes
import os
from pathlib import Path
from typing import Iterable

import boto3
from botocore.exceptions import ClientError

from platform.secretstore.loader import load_secretstore, env_for_integration


def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def normalize_prefix(prefix: str) -> str:
    p = str(prefix or "").strip().lstrip("/")
    if p and not p.endswith("/"):
        p = p + "/"
    return p


def guess_content_type(path: Path) -> str:
    ct, _ = mimetypes.guess_type(str(path))
    return str(ct or "").strip()


def upload_one(*, client, bucket: str, key: str, local_path: Path, content_type: str) -> None:
    extra = {}
    if content_type:
        extra["ContentType"] = content_type
    if extra:
        client.upload_file(str(local_path), bucket, key, ExtraArgs=extra)
    else:
        client.upload_file(str(local_path), bucket, key)


def verify_exists(*, client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dist-dir", default="dist_artifacts")
    ap.add_argument("--bucket", default="")
    ap.add_argument("--prefix", default="")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument(
        "--skip-if-missing-bucket",
        action="store_true",
        help="If no bucket is configured (args/env/secretstore), exit 0 and do nothing.",
    )
    args = ap.parse_args(argv)

    dist_dir = Path(args.dist_dir)
    if not dist_dir.exists():
        print(f"dist-dir not found: {dist_dir}")
        return 2

    repo_root = Path(__file__).resolve().parents[1]
    store = load_secretstore(repo_root)
    integ = env_for_integration(store, "artifact_store_s3")

    bucket = str(args.bucket or "").strip()
    if not bucket:
        bucket = str(os.environ.get("PLATFORM_ARTIFACTS_S3_BUCKET", "") or "").strip()
    if not bucket:
        bucket = str(integ.get("PLATFORM_ARTIFACTS_S3_BUCKET", "") or integ.get("AWS_S3_BUCKET", "") or "").strip()
    if not bucket:
        if args.skip_if_missing_bucket:
            print("S3 bucket not configured; skipping upload.")
            return 0
        print("S3 bucket is required. Provide --bucket or set PLATFORM_ARTIFACTS_S3_BUCKET.")
        return 2

    prefix = normalize_prefix(
        str(args.prefix or "").strip()
        or str(os.environ.get("PLATFORM_ARTIFACTS_S3_PREFIX", "") or "").strip()
        or str(integ.get("PLATFORM_ARTIFACTS_S3_PREFIX", "") or "").strip()
    )

    region = str(os.environ.get("AWS_REGION", "") or os.environ.get("AWS_DEFAULT_REGION", "") or "").strip()
    if not region:
        region = str(integ.get("AWS_REGION", "") or integ.get("AWS_DEFAULT_REGION", "") or "").strip()

    # Treat placeholder values as unset
    if region.upper() == 'REPLACE_ME':
        region = ''

    ak = str(os.environ.get("AWS_ACCESS_KEY_ID", "") or "").strip() or str(integ.get("AWS_ACCESS_KEY_ID", "") or "").strip()
    sk = str(os.environ.get("AWS_SECRET_ACCESS_KEY", "") or "").strip() or str(integ.get("AWS_SECRET_ACCESS_KEY", "") or "").strip()
    st = str(os.environ.get("AWS_SESSION_TOKEN", "") or "").strip() or str(integ.get("AWS_SESSION_TOKEN", "") or "").strip()

    session_kwargs = {}
    if region:
        session_kwargs["region_name"] = region
    if ak and sk:
        session_kwargs["aws_access_key_id"] = ak
        session_kwargs["aws_secret_access_key"] = sk
        if st:
            session_kwargs["aws_session_token"] = st

    if session_kwargs:
        session = boto3.session.Session(**session_kwargs)
        client = session.client("s3")
    else:
        client = boto3.client("s3")

    uploaded = 0
    verified = 0
    for p in iter_files(dist_dir):
        rel = p.relative_to(dist_dir).as_posix()
        key = f"{prefix}{rel}"
        ct = guess_content_type(p)
        print(f"upload: {p} -> s3://{bucket}/{key}")
        upload_one(client=client, bucket=bucket, key=key, local_path=p, content_type=ct)
        uploaded += 1
        if args.verify:
            ok = verify_exists(client=client, bucket=bucket, key=key)
            if not ok:
                print(f"verify failed: s3://{bucket}/{key}")
                return 3
            verified += 1

    print(f"uploaded={uploaded} verified={verified}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
