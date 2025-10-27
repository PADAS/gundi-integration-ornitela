from .core import PullActionConfiguration
import pydantic


class ProcessTelemetryDataActionConfiguration(PullActionConfiguration):
    bucket_name: str = pydantic.Field(..., title="Bucket Name", description="Google Cloud Storage bucket name")
    bucket_path: str = pydantic.Field("", title="Bucket Path", description="Path within the bucket where telemetry files are stored")
    credentials_file: str = pydantic.Field(..., title="Credentials File", description="Path to Google Cloud service account credentials JSON file")
    archive_days: int = pydantic.Field(30, title="Archive Days", description="Number of days after processing before files are archived")
    delete_after_archive_days: int = pydantic.Field(90, title="Delete After Archive Days", description="Number of days after archiving before files are deleted")
    historical_limit_days: int = pydantic.Field(30, title="Historical Limit Days", description="Number of days to look back for data")
    @pydantic.validator("bucket_path")
    def validate_bucket_path(cls, v):
        return v.strip().strip('/') if v else ""
