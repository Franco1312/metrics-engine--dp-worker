"""Expression evaluator."""

import operator
from typing import Callable

import pandas as pd

from metrics_worker.application.services.window_ops import (
    ema,
    lag,
    sma,
    window_max,
    window_min,
    window_sum,
)
from metrics_worker.domain.enums import CompositeOp, ExpressionType, SeriesMathOp, WindowOp
from metrics_worker.domain.errors import ExpressionEvaluationError, InvalidExpressionError
from metrics_worker.domain.types import ExpressionResult, SeriesFrame

# Strategy pattern: Map expression types to evaluators
_EXPRESSION_EVALUATORS: dict[ExpressionType, Callable] = {}


def _register_evaluator(expr_type: ExpressionType, evaluator: Callable) -> None:
    """Register an expression evaluator."""
    _EXPRESSION_EVALUATORS[expr_type] = evaluator


def _infer_expression_type_from_op(op: str, expression: dict[str, any] | None = None) -> ExpressionType:
    """Infer expression type from operation string and expression structure.
    
    Uses structure to disambiguate operations that exist in multiple enums:
    - window_op: requires "series" and "window" fields
    - composite: requires "operands" array field
    - series_math: requires "left" and "right" fields
    """
    # If expression structure is provided, use it to disambiguate
    if expression is not None:
        # window_op must have "series" and "window" fields
        if "series" in expression and "window" in expression:
            try:
                WindowOp(op)
                return ExpressionType.WINDOW_OP
            except ValueError:
                pass
        
        # composite must have "operands" array field
        if "operands" in expression:
            try:
                CompositeOp(op)
                return ExpressionType.COMPOSITE
            except ValueError:
                pass
        
        # series_math must have "left" and "right" fields
        if "left" in expression and "right" in expression:
            try:
                SeriesMathOp(op)
                return ExpressionType.SERIES_MATH
            except ValueError:
                pass
    
    # Fallback: try enums in order of specificity
    # Check WindowOp first (but only if structure matches)
    if expression is None or ("series" in expression and "window" in expression):
        try:
            WindowOp(op)
            return ExpressionType.WINDOW_OP
        except ValueError:
            pass
    
    # Check SeriesMathOp
    try:
        SeriesMathOp(op)
        return ExpressionType.SERIES_MATH
    except ValueError:
        pass
    
    # Check CompositeOp
    try:
        CompositeOp(op)
        return ExpressionType.COMPOSITE
    except ValueError:
        raise InvalidExpressionError(f"Unknown operation: {op}")


def evaluate_expression(
    expression: dict[str, any],
    expression_type: ExpressionType | str,
    series_data: dict[str, pd.DataFrame],
) -> ExpressionResult:
    """Evaluate metric expression."""
    # Convert string to enum if needed (for backward compatibility)
    if isinstance(expression_type, str):
        try:
            expression_type = ExpressionType(expression_type)
        except ValueError:
            raise InvalidExpressionError(f"Unknown expression type: {expression_type}")
    
    evaluator = _EXPRESSION_EVALUATORS.get(expression_type)
    if not evaluator:
        raise InvalidExpressionError(f"Unknown expression type: {expression_type}")
    
    return evaluator(expression, series_data)


# Series math operations mapping
_SERIES_MATH_OPS: dict[SeriesMathOp, Callable[[pd.Series, pd.Series], pd.Series]] = {
    SeriesMathOp.ADD: operator.add,
    SeriesMathOp.SUBTRACT: operator.sub,
    SeriesMathOp.MULTIPLY: operator.mul,
    SeriesMathOp.RATIO: operator.truediv,
}


def _evaluate_series_math(
    expression: dict[str, any],
    series_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Evaluate series_math expression."""
    op_str = expression.get("op")
    if not op_str:
        raise InvalidExpressionError("Missing operation in series_math expression")
    
    try:
        op = SeriesMathOp(op_str)
    except ValueError:
        raise InvalidExpressionError(f"Unknown series_math op: {op_str}")

    left = _resolve_operand(expression.get("left"), series_data)
    right = _resolve_operand(expression.get("right"), series_data)

    result_df = _align_series(left, right)

    # Apply operation using operator mapping
    operation = _SERIES_MATH_OPS.get(op)
    if not operation:
        raise InvalidExpressionError(f"Unsupported series_math operation: {op}")
    
    result_df["value"] = operation(result_df["left_value"], result_df["right_value"])

    # Apply scale if present
    scale = expression.get("scale")
    if scale is not None:
        result_df["value"] = result_df["value"] * scale

    return result_df[["obs_time", "value"]].copy()


_register_evaluator(ExpressionType.SERIES_MATH, _evaluate_series_math)


# Window operations mapping
_WINDOW_OPS: dict[WindowOp, Callable[[pd.Series, int], pd.Series]] = {
    WindowOp.SMA: sma,
    WindowOp.EMA: ema,
    WindowOp.SUM: window_sum,
    WindowOp.MAX: window_max,
    WindowOp.MIN: window_min,
    WindowOp.LAG: lag,
}


def _evaluate_window_op(
    expression: dict[str, any],
    series_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Evaluate window_op expression."""
    op_str = expression.get("op")
    if not op_str:
        raise InvalidExpressionError("Missing operation in window_op expression")
    
    try:
        op = WindowOp(op_str)
    except ValueError:
        raise InvalidExpressionError(f"Unknown window_op: {op_str}")

    window = expression.get("window")
    if not isinstance(window, int) or window < 1:
        raise InvalidExpressionError(f"Invalid window: {window}")

    series = _resolve_operand(expression.get("series"), series_data)

    series_df = series[["obs_time", "value"]].copy().sort_values("obs_time")
    series_df = series_df.set_index("obs_time")

    # Apply window operation using function mapping
    window_func = _WINDOW_OPS.get(op)
    if not window_func:
        raise InvalidExpressionError(f"Unsupported window operation: {op}")
    
    result = window_func(series_df["value"], window)

    result_df = pd.DataFrame({"obs_time": result.index, "value": result.values})
    return result_df


_register_evaluator(ExpressionType.WINDOW_OP, _evaluate_window_op)


# Composite operations mapping
_COMPOSITE_OPS: dict[CompositeOp, Callable[[pd.DataFrame], pd.Series]] = {
    CompositeOp.SUM: lambda df: df.sum(axis=1),
    CompositeOp.AVG: lambda df: df.mean(axis=1),
    CompositeOp.MAX: lambda df: df.max(axis=1),
    CompositeOp.MIN: lambda df: df.min(axis=1),
}


def _evaluate_composite(
    expression: dict[str, any],
    series_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Evaluate composite expression."""
    op_str = expression.get("op")
    if not op_str:
        raise InvalidExpressionError("Missing operation in composite expression")
    
    try:
        op = CompositeOp(op_str)
    except ValueError:
        raise InvalidExpressionError(f"Unknown composite op: {op_str}")

    operands = expression.get("operands", [])
    if len(operands) < 2:
        raise InvalidExpressionError("Composite requires at least 2 operands")

    resolved = [_resolve_operand(op, series_data) for op in operands]

    result_df = _align_multiple_series(resolved)

    value_cols = [col for col in result_df.columns if col.startswith("value_")]

    # Apply composite operation using function mapping
    composite_func = _COMPOSITE_OPS.get(op)
    if not composite_func:
        raise InvalidExpressionError(f"Unsupported composite operation: {op}")
    
    result_df["value"] = composite_func(result_df[value_cols])

    return result_df[["obs_time", "value"]].copy()


_register_evaluator(ExpressionType.COMPOSITE, _evaluate_composite)


def _resolve_operand(
    operand: dict[str, any] | None,
    series_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Resolve operand to DataFrame."""
    if operand is None:
        raise InvalidExpressionError("Missing operand")

    # Direct series reference
    # Support both snake_case and camelCase for series_code
    series_code = operand.get("series_code") or operand.get("seriesCode")
    if series_code:
        if series_code not in series_data:
            raise ExpressionEvaluationError(f"Series not found: {series_code}")
        return series_data[series_code].copy()

    # Nested expression
    if "op" in operand:
        op_str = operand.get("op")
        if not op_str:
            raise InvalidExpressionError("Missing operation in operand")
        
        # Infer expression type from operation and expression structure
        expr_type = _infer_expression_type_from_op(op_str, operand)
        evaluator = _EXPRESSION_EVALUATORS.get(expr_type)
        if not evaluator:
            raise InvalidExpressionError(f"Cannot evaluate operand with type: {expr_type}")
        
        return evaluator(operand, series_data)

    raise InvalidExpressionError(f"Cannot resolve operand: {operand}")


def _align_series(
    left: pd.DataFrame,
    right: pd.DataFrame,
) -> pd.DataFrame:
    """Align two series by obs_time."""
    left_df = left[["obs_time", "value"]].copy()
    right_df = right[["obs_time", "value"]].copy()

    merged = pd.merge(
        left_df,
        right_df,
        on="obs_time",
        how="outer",
        suffixes=("_left", "_right"),
    ).sort_values("obs_time")

    merged = merged.rename(columns={"value_left": "left_value", "value_right": "right_value"})
    merged["obs_time"] = merged["obs_time"]

    return merged


def _align_multiple_series(series_list: list[pd.DataFrame]) -> pd.DataFrame:
    """Align multiple series by obs_time."""
    if not series_list:
        raise InvalidExpressionError("Empty series list")

    result = series_list[0][["obs_time", "value"]].copy()
    result = result.rename(columns={"value": "value_0"})

    for idx, series_df in enumerate(series_list[1:], start=1):
        series_df = series_df[["obs_time", "value"]].copy()
        result = pd.merge(
            result,
            series_df,
            on="obs_time",
            how="outer",
            suffixes=("", f"_{idx}"),
        )
        if f"value_{idx}" not in result.columns:
            result = result.rename(columns={"value": f"value_{idx}"})

    result = result.sort_values("obs_time")
    return result

