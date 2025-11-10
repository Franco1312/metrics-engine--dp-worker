"""SQS consumer for metric run requests."""

import json

import boto3
from botocore.exceptions import ClientError

from metrics_worker.application.dto.events import MetricRunRequestedEvent
from metrics_worker.infrastructure.config.settings import Settings


class SQSConsumer:
    """SQS consumer for metric run requests."""

    def __init__(self, settings: Settings) -> None:
        """Initialize SQS client."""
        self.sqs_client = boto3.client("sqs", region_name=settings.aws_region)
        self.queue_url = settings.aws_sqs_run_request_queue_url

    async def receive_message(self) -> tuple[MetricRunRequestedEvent | None, str | None]:
        """Receive and parse message from SQS.
        
        Returns:
            Tuple of (event, receipt_handle) or (None, None) if no message.
        """
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                MessageAttributeNames=["All"],
            )

            messages = response.get("Messages", [])
            if not messages:
                return None, None

            message = messages[0]
            receipt_handle = message["ReceiptHandle"]
            body = json.loads(message["Body"])

            event = self._parse_event(body)
            return event, receipt_handle

        except ClientError as e:
            raise RuntimeError(f"Failed to receive message from SQS: {e}") from e
        except (KeyError, json.JSONDecodeError, ValueError) as e:
            raise RuntimeError(f"Failed to parse message: {e}") from e

    def _parse_event(self, body: dict) -> MetricRunRequestedEvent:
        """Parse event from SQS message body."""
        if body.get("Type") == "Notification":
            return self._parse_sns_message(body)
        return self._parse_direct_message(body)

    def _parse_sns_message(self, sns_body: dict) -> MetricRunRequestedEvent:
        """Parse event from SNS-wrapped message."""
        sns_message = json.loads(sns_body["Message"])
        message_attributes = sns_body.get("MessageAttributes", {})

        event_data = dict(sns_message)
        self._apply_message_attributes(event_data, message_attributes)

        event = MetricRunRequestedEvent(**event_data)
        self._validate_event_type(event)
        return event

    def _parse_direct_message(self, body: dict) -> MetricRunRequestedEvent:
        """Parse event from direct SQS message."""
        event = MetricRunRequestedEvent(**body)
        self._validate_event_type(event)
        return event

    def _apply_message_attributes(self, event_data: dict, message_attributes: dict) -> None:
        """Apply SNS message attributes to event data."""
        if not message_attributes:
            return

        type_attr = message_attributes.get("type", {}).get("Value")
        metric_code_attr = message_attributes.get("metricCode", {}).get("Value")

        if type_attr:
            event_data["type"] = type_attr
        if metric_code_attr:
            event_data["metricCode"] = metric_code_attr

    def _validate_event_type(self, event: MetricRunRequestedEvent) -> None:
        """Validate that event type is metric_run_requested."""
        if event.type != "metric_run_requested":
            raise ValueError(f"Unexpected event type: {event.type}")

    async def delete_message(self, receipt_handle: str) -> None:
        """Delete message from SQS."""
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )
        except ClientError as e:
            raise RuntimeError(f"Failed to delete message from SQS: {e}") from e
