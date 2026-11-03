from pathlib import Path


class S3DownloadFileStore:
    """Download files in an S3 bucket, handed to users as presigned URLs."""

    def __init__(self, s3_client, bucket: str):
        self._s3 = s3_client
        self._bucket = bucket

    async def write_file(self, key: str, path: Path, content_type: str) -> None:
        """Upload a local file to key."""
        await self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=path.read_bytes(),
            ContentType=content_type,
        )

    async def presign_download_url(
        self, key: str, filename: str, expires_seconds: int
    ) -> str:
        """Create a URL that downloads key as filename until it expires."""
        return await self._s3.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self._bucket,
                "Key": key,
                "ResponseContentDisposition": f'attachment; filename="{filename}"',
            },
            ExpiresIn=expires_seconds,
        )
