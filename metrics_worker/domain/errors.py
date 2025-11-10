"""Domain errors."""


class DomainError(Exception):
    """Base domain error."""


class ExpressionEvaluationError(DomainError):
    """Error evaluating metric expression."""


class SeriesNotFoundError(DomainError):
    """Series not found in dataset."""


class InvalidExpressionError(DomainError):
    """Invalid expression structure."""


class ManifestValidationError(DomainError):
    """Output manifest validation failed."""

