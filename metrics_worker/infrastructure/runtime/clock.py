"""Clock implementation."""

from datetime import datetime

from metrics_worker.domain.ports import ClockPort
from metrics_worker.domain.types import Timestamp


class SystemClock(ClockPort):
    """System clock implementation."""

    def now(self) -> Timestamp:
        """Get current timestamp."""
        return datetime.utcnow()

    def format_version_ts(self, ts: Timestamp) -> str:
        """Format timestamp as version_ts string."""
        return ts.strftime("%Y-%m-%dT%H-%M-%S")

