"""SQS run worker adapter."""

from metrics_worker.application.use_cases.handle_run_request import run as handle_run
from metrics_worker.domain.ports import (
    CatalogPort,
    ClockPort,
    DataReaderPort,
    EventBusPort,
    OutputWriterPort,
)
from metrics_worker.infrastructure.aws.sqs_consumer import SQSConsumer


class SQSRunWorker:
    """SQS run worker adapter."""

    def __init__(
        self,
        sqs_consumer: SQSConsumer,
        catalog: CatalogPort,
        data_reader: DataReaderPort,
        output_writer: OutputWriterPort,
        event_bus: EventBusPort,
        clock: ClockPort,
    ) -> None:
        """Initialize SQS run worker."""
        self.sqs_consumer = sqs_consumer
        self.catalog = catalog
        self.data_reader = data_reader
        self.output_writer = output_writer
        self.event_bus = event_bus
        self.clock = clock

    async def process_next_message(self) -> bool:
        """Process next message from SQS. Returns True if message was processed."""
        event, receipt_handle = await self.sqs_consumer.receive_message()

        if event is None:
            return False

        try:
            await handle_run(
                event,
                self.catalog,
                self.data_reader,
                self.output_writer,
                self.event_bus,
                self.clock,
            )
            await self.sqs_consumer.delete_message(receipt_handle)
            return True
        except Exception:
            await self.sqs_consumer.delete_message(receipt_handle)
            raise

