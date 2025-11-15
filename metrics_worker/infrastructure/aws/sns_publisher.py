"""SNS event publisher for Control Plane events."""

import json

import boto3
import structlog
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

from metrics_worker.domain.ports import EventBusPort
from metrics_worker.domain.types import Timestamp
from metrics_worker.infrastructure.config.settings import Settings

logger = structlog.get_logger()


class SNSPublisher(EventBusPort):
    """SNS event publisher for Control Plane events.
    
    Publishes events to SNS topics. The Control Plane consumes from SQS queues
    that are subscribed to these SNS topics.
    """

    def __init__(self, settings: Settings) -> None:
        """Initialize SNS client."""
        self.settings = settings
        self.sns_client = boto3.client("sns", region_name=settings.aws_region)
        # SNS Topics for each event type (all required)
        self.started_topic_arn = settings.aws_sns_metric_run_started_topic_arn
        self.heartbeat_topic_arn = settings.aws_sns_metric_run_heartbeat_topic_arn
        self.completed_topic_arn = settings.aws_sns_metric_run_completed_topic_arn

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def publish_started(
        self,
        run_id: str,
        metric_code: str,
        started_at: Timestamp,
    ) -> None:
        """Publish metric_run_started event to SNS."""
        event = {
            "type": "metric_run_started",
            "runId": run_id,
            "metricCode": metric_code,
            "startedAt": started_at.isoformat() + "Z",
        }

        await self._publish(self.started_topic_arn, event, run_id, "metric_run_started", metric_code)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def publish_heartbeat(
        self,
        run_id: str,
        metric_code: str,
        progress: float,
        ts: Timestamp,
    ) -> None:
        """Publish metric_run_heartbeat event to SNS."""
        event = {
            "type": "metric_run_heartbeat",
            "runId": run_id,
            "metricCode": metric_code,
            "progress": progress,
            "ts": ts.isoformat() + "Z",
        }

        await self._publish(self.heartbeat_topic_arn, event, run_id, "metric_run_heartbeat", metric_code)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def publish_completed(
        self,
        run_id: str,
        metric_code: str,
        status: str,
        version_ts: str | None = None,
        output_manifest: str | None = None,
        row_count: int | None = None,
        error: str | None = None,
    ) -> None:
        """Publish metric_run_completed event to SNS."""
        event = {
            "type": "metric_run_completed",
            "runId": run_id,
            "metricCode": metric_code,
            "status": status,
        }

        if status == "SUCCESS":
            if version_ts:
                event["versionTs"] = version_ts
            if output_manifest:
                event["outputManifest"] = output_manifest
            if row_count is not None:
                event["rowCount"] = row_count
        else:
            if error:
                event["error"] = error

        await self._publish(self.completed_topic_arn, event, run_id, "metric_run_completed", metric_code)

    async def _publish(
        self,
        topic_arn: str,
        event: dict,
        run_id: str,
        event_type: str,
        metric_code: str,
    ) -> None:
        """Publish event to SNS topic."""
        try:
            message = json.dumps(event)
            message_attributes = {
                "type": {"DataType": "String", "StringValue": event_type},
                "metricCode": {"DataType": "String", "StringValue": metric_code},
            }

            publish_params = {
                "TopicArn": topic_arn,
                "Message": message,
                "MessageAttributes": message_attributes,
            }

            # For FIFO topics, add MessageGroupId and MessageDeduplicationId
            if topic_arn.endswith(".fifo"):
                publish_params["MessageGroupId"] = run_id
                publish_params["MessageDeduplicationId"] = f"{run_id}:{event_type}"

            logger.info(
                "publishing_event_to_sns",
                event_type=event_type,
                run_id=run_id,
                metric_code=metric_code,
                topic_arn=topic_arn,
                status=event.get("status"),
            )

            response = self.sns_client.publish(**publish_params)
            
            logger.info(
                "event_published_to_sns",
                event_type=event_type,
                run_id=run_id,
                metric_code=metric_code,
                message_id=response.get("MessageId"),
                topic_arn=topic_arn,
            )
        except ClientError as e:
            logger.error(
                "failed_to_publish_event_to_sns",
                event_type=event_type,
                run_id=run_id,
                metric_code=metric_code,
                topic_arn=topic_arn,
                error=str(e),
                error_code=e.response.get("Error", {}).get("Code"),
            )
            raise RuntimeError(f"Failed to publish event to SNS: {e}") from e

