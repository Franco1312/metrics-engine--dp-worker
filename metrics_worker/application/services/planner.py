"""Planning service for data reads."""

from collections import defaultdict

from metrics_worker.domain.enums import CompositeOp, ExpressionType, SeriesMathOp, WindowOp
from metrics_worker.domain.types import ExpressionJson


class ReadPlan:
    """Plan for reading series data."""

    def __init__(self) -> None:
        """Initialize read plan."""
        self.series_by_dataset: dict[str, list[str]] = defaultdict(list)
        self.columns: set[str] = {"obs_time", "value", "internal_series_code"}

    def add_series(self, dataset_id: str, series_code: str) -> None:
        """Add series to read plan."""
        self.series_by_dataset[dataset_id].append(series_code)

    def get_series_codes(self, dataset_id: str) -> list[str]:
        """Get series codes for dataset."""
        return self.series_by_dataset.get(dataset_id, [])


def plan_reads(
    expression: ExpressionJson,
    expression_type: ExpressionType | str,
    inputs: list[dict[str, str]],
) -> ReadPlan:
    """Plan data reads for expression evaluation."""
    plan = ReadPlan()

    for input_item in inputs:
        dataset_id = input_item["datasetId"]
        series_code = input_item["seriesCode"]
        plan.add_series(dataset_id, series_code)

    # Convert string to enum if needed
    if isinstance(expression_type, str):
        expression_type = ExpressionType(expression_type)
    
    _extract_series_from_expression(expression, expression_type, plan)

    return plan


def _extract_series_from_expression(
    expression: ExpressionJson,
    expression_type: ExpressionType,
    plan: ReadPlan,
) -> None:
    """Recursively extract series references from expression."""
    if expression_type == ExpressionType.SERIES_MATH:
        left = expression.get("left", {})
        right = expression.get("right", {})
        if "series_code" in left:
            pass
        elif "op" in left:
            _extract_series_from_expression(left, _infer_type(left), plan)
        if "series_code" in right:
            pass
        elif "op" in right:
            _extract_series_from_expression(right, _infer_type(right), plan)

    elif expression_type == ExpressionType.WINDOW_OP:
        series = expression.get("series", {})
        if "series_code" in series:
            pass
        elif "op" in series:
            _extract_series_from_expression(series, _infer_type(series), plan)

    elif expression_type == ExpressionType.COMPOSITE:
        operands = expression.get("operands", [])
        for operand in operands:
            if "series_code" in operand:
                pass
            elif "op" in operand:
                _extract_series_from_expression(operand, _infer_type(operand), plan)


def _infer_type(expr: ExpressionJson) -> ExpressionType:
    """Infer expression type from structure.
    
    Uses structure to disambiguate operations that exist in multiple enums:
    - window_op: requires "series" and "window" fields
    - composite: requires "operands" array field
    - series_math: requires "left" and "right" fields
    """
    op = expr.get("op", "")
    if not op:
        return ExpressionType.SERIES_MATH  # Default fallback
    
    # Use structure to disambiguate operations that exist in multiple enums
    # window_op must have "series" and "window" fields
    if "series" in expr and "window" in expr:
        try:
            WindowOp(op)
            return ExpressionType.WINDOW_OP
        except ValueError:
            pass
    
    # composite must have "operands" array field
    if "operands" in expr:
        try:
            CompositeOp(op)
            return ExpressionType.COMPOSITE
        except ValueError:
            pass
    
    # series_math must have "left" and "right" fields
    if "left" in expr and "right" in expr:
        try:
            SeriesMathOp(op)
            return ExpressionType.SERIES_MATH
        except ValueError:
            pass
    
    # Fallback: try enums in order of specificity
    # Check WindowOp first (but only if structure matches)
    if "series" in expr and "window" in expr:
        try:
            WindowOp(op)
            return ExpressionType.WINDOW_OP
        except ValueError:
            pass
    
    try:
        SeriesMathOp(op)
        return ExpressionType.SERIES_MATH
    except ValueError:
        pass
    
    try:
        CompositeOp(op)
        return ExpressionType.COMPOSITE
    except ValueError:
        pass
    
    # Default fallback
    return ExpressionType.SERIES_MATH

