"""Domain enums for expression types and operations."""

from enum import Enum


class ExpressionType(str, Enum):
    """Expression type enum."""

    SERIES_MATH = "series_math"
    WINDOW_OP = "window_op"
    COMPOSITE = "composite"


class SeriesMathOp(str, Enum):
    """Series math operation enum."""

    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    RATIO = "ratio"


class WindowOp(str, Enum):
    """Window operation enum."""

    SMA = "sma"  # Simple Moving Average
    EMA = "ema"  # Exponential Moving Average
    SUM = "sum"
    MAX = "max"
    MIN = "min"
    LAG = "lag"


class CompositeOp(str, Enum):
    """Composite operation enum."""

    SUM = "sum"
    AVG = "avg"
    MAX = "max"
    MIN = "min"

