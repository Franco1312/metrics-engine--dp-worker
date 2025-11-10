"""Application settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    aws_region: str = "us-east-1"
    aws_s3_bucket: str
    aws_sqs_run_request_queue_url: str
    aws_sqs_run_request_queue_enabled: bool = True
    # SNS Topics for publishing events to Control Plane (all required)
    # The Control Plane consumes from SQS queues subscribed to these SNS topics
    aws_sns_metric_run_started_topic_arn: str
    aws_sns_metric_run_heartbeat_topic_arn: str
    aws_sns_metric_run_completed_topic_arn: str
    worker_heartbeat_interval_seconds: int = 30
    output_compression: str = "snappy"
    prometheus_port: int = 9300

    # AWS Credentials (optional - loaded from .env but not used directly)
    # These are automatically picked up by boto3 from environment variables
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_prefix="",
        # Allow extra fields to be loaded but not validated
        extra="ignore",
    )

