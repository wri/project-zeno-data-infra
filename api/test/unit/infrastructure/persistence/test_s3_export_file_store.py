import pytest

from app.infrastructure.persistence.s3_export_file_store import S3ExportFileStore


class RecordingS3Client:
    """Records uploads from disk, and can't accept a file sent from memory in one
    request."""

    def __init__(self):
        self.uploads = []

    async def upload_file(self, Filename, Bucket, Key, ExtraArgs=None):
        self.uploads.append((Filename, Bucket, Key, ExtraArgs))


@pytest.mark.asyncio
async def test_files_are_uploaded_from_disk_with_their_content_type(tmp_path):
    path = tmp_path / "export.csv"
    path.write_text("lat,lon\n1.0,2.0\n")
    client = RecordingS3Client()

    await S3ExportFileStore(client, "exports").write_file("key.csv", path, "text/csv")

    assert client.uploads == [
        (str(path), "exports", "key.csv", {"ContentType": "text/csv"})
    ]
