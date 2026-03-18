from __future__ import annotations

import argparse
import mimetypes
import os
from pathlib import Path

import boto3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upload a local file to a private Cloudflare R2 bucket."
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Local file to upload, e.g. data/serving/dataset_visibility_app.parquet",
    )
    parser.add_argument(
        "--key",
        required=True,
        help="Destination object key in R2, e.g. datasets/dataset_visibility_app.parquet",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("R2_BUCKET"),
        help="R2 bucket name. Defaults to env var R2_BUCKET.",
    )
    parser.add_argument(
        "--account-id",
        default=os.getenv("R2_ACCOUNT_ID"),
        help="Cloudflare account ID. Defaults to env var R2_ACCOUNT_ID.",
    )
    parser.add_argument(
        "--access-key-id",
        default=os.getenv("R2_ACCESS_KEY_ID"),
        help="R2 access key ID. Defaults to env var R2_ACCESS_KEY_ID.",
    )
    parser.add_argument(
        "--secret-access-key",
        default=os.getenv("R2_SECRET_ACCESS_KEY"),
        help="R2 secret access key. Defaults to env var R2_SECRET_ACCESS_KEY.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.getenv("R2_ENDPOINT_URL"),
        help="Full R2 endpoint URL. If omitted, built from account ID.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("R2_REGION", "auto"),
        help="Region name for R2 client. Defaults to 'auto'.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    local_file = Path(args.file)
    if not local_file.exists():
        raise FileNotFoundError(f"Local file not found: {local_file}")

    if not args.bucket:
        raise ValueError("Missing bucket. Use --bucket or env var R2_BUCKET.")
    if not args.access_key_id:
        raise ValueError(
            "Missing access key ID. Use --access-key-id or env var R2_ACCESS_KEY_ID."
        )
    if not args.secret_access_key:
        raise ValueError(
            "Missing secret access key. Use --secret-access-key or env var R2_SECRET_ACCESS_KEY."
        )

    endpoint_url = args.endpoint_url
    if not endpoint_url:
        if not args.account_id:
            raise ValueError(
                "Missing endpoint URL and account ID. "
                "Provide --endpoint-url or --account-id / env vars."
            )
        endpoint_url = f"https://{args.account_id}.r2.cloudflarestorage.com"

    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=args.access_key_id,
        aws_secret_access_key=args.secret_access_key,
        region_name=args.region,
    )

    content_type, _ = mimetypes.guess_type(str(local_file))
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    print(f"Uploading {local_file} to s3://{args.bucket}/{args.key} ...")
    if extra_args:
        client.upload_file(
            str(local_file),
            args.bucket,
            args.key,
            ExtraArgs=extra_args,
        )
    else:
        client.upload_file(
            str(local_file),
            args.bucket,
            args.key,
        )

    print("Upload completed successfully.")


if __name__ == "__main__":
    main()