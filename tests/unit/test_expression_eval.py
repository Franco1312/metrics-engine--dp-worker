"""Unit tests for expression evaluator."""

import pandas as pd
import pytest

from metrics_worker.application.services.expression_eval import evaluate_expression
from metrics_worker.domain.errors import ExpressionEvaluationError, InvalidExpressionError


def test_series_math_add():
    """Test series_math add operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [1.0, 2.0, 3.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [4.0, 5.0, 6.0]}),
    }

    expression = {"op": "add", "left": {"series_code": "A"}, "right": {"series_code": "B"}}
    result = evaluate_expression(expression, "series_math", series_data)

    assert len(result) == 3
    assert result["value"].iloc[0] == 5.0
    assert result["value"].iloc[1] == 7.0
    assert result["value"].iloc[2] == 9.0


def test_series_math_ratio():
    """Test series_math ratio operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [10.0, 20.0, 30.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [2.0, 4.0, 5.0]}),
    }

    expression = {"op": "ratio", "left": {"series_code": "A"}, "right": {"series_code": "B"}}
    result = evaluate_expression(expression, "series_math", series_data)

    assert result["value"].iloc[0] == 5.0
    assert result["value"].iloc[1] == 5.0
    assert result["value"].iloc[2] == 6.0


def test_series_math_with_scale():
    """Test series_math with scale factor."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [3.0, 4.0]}),
    }

    expression = {
        "op": "add",
        "left": {"series_code": "A"},
        "right": {"series_code": "B"},
        "scale": 2.0,
    }
    result = evaluate_expression(expression, "series_math", series_data)

    assert result["value"].iloc[0] == 8.0
    assert result["value"].iloc[1] == 12.0


def test_window_op_sma():
    """Test window_op with SMA."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=5), "value": [1.0, 2.0, 3.0, 4.0, 5.0]}),
    }

    expression = {"op": "sma", "series": {"series_code": "A"}, "window": 3}
    result = evaluate_expression(expression, "window_op", series_data)

    assert len(result) == 5
    assert pd.isna(result["value"].iloc[0])
    assert pd.isna(result["value"].iloc[1])
    assert result["value"].iloc[2] == 2.0


def test_composite_sum():
    """Test composite sum operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [1.0, 2.0, 3.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [4.0, 5.0, 6.0]}),
        "C": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [7.0, 8.0, 9.0]}),
    }

    expression = {
        "op": "sum",
        "operands": [{"series_code": "A"}, {"series_code": "B"}, {"series_code": "C"}],
    }
    result = evaluate_expression(expression, "composite", series_data)

    assert result["value"].iloc[0] == 12.0
    assert result["value"].iloc[1] == 15.0
    assert result["value"].iloc[2] == 18.0


def test_missing_series():
    """Test missing series error."""
    series_data = {"A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]})}

    expression = {"op": "add", "left": {"series_code": "A"}, "right": {"series_code": "MISSING"}}

    with pytest.raises(ExpressionEvaluationError):
        evaluate_expression(expression, "series_math", series_data)


def test_invalid_expression_type():
    """Test invalid expression type."""
    series_data = {"A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]})}

    with pytest.raises(InvalidExpressionError):
        evaluate_expression({}, "unknown_type", series_data)


def test_series_math_subtract():
    """Test series_math subtract operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [10.0, 20.0, 30.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [4.0, 5.0, 6.0]}),
    }

    expression = {"op": "subtract", "left": {"series_code": "A"}, "right": {"series_code": "B"}}
    result = evaluate_expression(expression, "series_math", series_data)

    assert result["value"].iloc[0] == 6.0
    assert result["value"].iloc[1] == 15.0
    assert result["value"].iloc[2] == 24.0


def test_series_math_multiply():
    """Test series_math multiply operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [2.0, 3.0, 4.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [5.0, 6.0, 7.0]}),
    }

    expression = {"op": "multiply", "left": {"series_code": "A"}, "right": {"series_code": "B"}}
    result = evaluate_expression(expression, "series_math", series_data)

    assert result["value"].iloc[0] == 10.0
    assert result["value"].iloc[1] == 18.0
    assert result["value"].iloc[2] == 28.0


def test_series_math_missing_op():
    """Test series_math with missing operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [3.0, 4.0]}),
    }

    expression = {"left": {"series_code": "A"}, "right": {"series_code": "B"}}

    with pytest.raises(InvalidExpressionError, match="Missing operation"):
        evaluate_expression(expression, "series_math", series_data)


def test_series_math_invalid_op():
    """Test series_math with invalid operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [3.0, 4.0]}),
    }

    expression = {"op": "invalid_op", "left": {"series_code": "A"}, "right": {"series_code": "B"}}

    with pytest.raises(InvalidExpressionError):
        evaluate_expression(expression, "series_math", series_data)


def test_window_op_ema():
    """Test window_op with EMA."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=5), "value": [1.0, 2.0, 3.0, 4.0, 5.0]}),
    }

    expression = {"op": "ema", "series": {"series_code": "A"}, "window": 3}
    result = evaluate_expression(expression, "window_op", series_data)

    assert len(result) == 5
    assert not pd.isna(result["value"].iloc[0])


def test_window_op_sum():
    """Test window_op with sum."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=5), "value": [1.0, 2.0, 3.0, 4.0, 5.0]}),
    }

    expression = {"op": "sum", "series": {"series_code": "A"}, "window": 3}
    result = evaluate_expression(expression, "window_op", series_data)

    assert len(result) == 5
    assert pd.isna(result["value"].iloc[0])
    assert pd.isna(result["value"].iloc[1])
    assert result["value"].iloc[2] == 6.0
    assert result["value"].iloc[3] == 9.0
    assert result["value"].iloc[4] == 12.0


def test_window_op_max():
    """Test window_op with max."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=5), "value": [1.0, 5.0, 3.0, 2.0, 4.0]}),
    }

    expression = {"op": "max", "series": {"series_code": "A"}, "window": 3}
    result = evaluate_expression(expression, "window_op", series_data)

    assert len(result) == 5
    assert pd.isna(result["value"].iloc[0])
    assert pd.isna(result["value"].iloc[1])
    assert result["value"].iloc[2] == 5.0
    assert result["value"].iloc[3] == 5.0
    assert result["value"].iloc[4] == 4.0


def test_window_op_min():
    """Test window_op with min."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=5), "value": [5.0, 1.0, 3.0, 4.0, 2.0]}),
    }

    expression = {"op": "min", "series": {"series_code": "A"}, "window": 3}
    result = evaluate_expression(expression, "window_op", series_data)

    assert len(result) == 5
    assert pd.isna(result["value"].iloc[0])
    assert pd.isna(result["value"].iloc[1])
    assert result["value"].iloc[2] == 1.0
    assert result["value"].iloc[3] == 1.0
    assert result["value"].iloc[4] == 2.0


def test_window_op_lag():
    """Test window_op with lag."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=5), "value": [1.0, 2.0, 3.0, 4.0, 5.0]}),
    }

    expression = {"op": "lag", "series": {"series_code": "A"}, "window": 2}
    result = evaluate_expression(expression, "window_op", series_data)

    assert len(result) == 5
    assert pd.isna(result["value"].iloc[0])
    assert pd.isna(result["value"].iloc[1])
    assert result["value"].iloc[2] == 1.0
    assert result["value"].iloc[3] == 2.0
    assert result["value"].iloc[4] == 3.0


def test_window_op_missing_op():
    """Test window_op with missing operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
    }

    expression = {"series": {"series_code": "A"}, "window": 3}

    with pytest.raises(InvalidExpressionError, match="Missing operation"):
        evaluate_expression(expression, "window_op", series_data)


def test_window_op_invalid_op():
    """Test window_op with invalid operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
    }

    expression = {"op": "invalid_op", "series": {"series_code": "A"}, "window": 3}

    with pytest.raises(InvalidExpressionError):
        evaluate_expression(expression, "window_op", series_data)


def test_window_op_invalid_window():
    """Test window_op with invalid window."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
    }

    expression = {"op": "sma", "series": {"series_code": "A"}, "window": 0}

    with pytest.raises(InvalidExpressionError, match="Invalid window"):
        evaluate_expression(expression, "window_op", series_data)


def test_window_op_negative_window():
    """Test window_op with negative window."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
    }

    expression = {"op": "sma", "series": {"series_code": "A"}, "window": -1}

    with pytest.raises(InvalidExpressionError, match="Invalid window"):
        evaluate_expression(expression, "window_op", series_data)


def test_composite_avg():
    """Test composite avg operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [1.0, 2.0, 3.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [4.0, 5.0, 6.0]}),
        "C": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [7.0, 8.0, 9.0]}),
    }

    expression = {
        "op": "avg",
        "operands": [{"series_code": "A"}, {"series_code": "B"}, {"series_code": "C"}],
    }
    result = evaluate_expression(expression, "composite", series_data)

    assert result["value"].iloc[0] == 4.0  # (1+4+7)/3
    assert result["value"].iloc[1] == 5.0  # (2+5+8)/3
    assert result["value"].iloc[2] == 6.0  # (3+6+9)/3


def test_composite_max():
    """Test composite max operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [1.0, 5.0, 3.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [4.0, 2.0, 6.0]}),
        "C": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [2.0, 3.0, 1.0]}),
    }

    expression = {
        "op": "max",
        "operands": [{"series_code": "A"}, {"series_code": "B"}, {"series_code": "C"}],
    }
    result = evaluate_expression(expression, "composite", series_data)

    assert result["value"].iloc[0] == 4.0
    assert result["value"].iloc[1] == 5.0
    assert result["value"].iloc[2] == 6.0


def test_composite_min():
    """Test composite min operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [5.0, 2.0, 4.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [3.0, 6.0, 1.0]}),
        "C": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [4.0, 3.0, 5.0]}),
    }

    expression = {
        "op": "min",
        "operands": [{"series_code": "A"}, {"series_code": "B"}, {"series_code": "C"}],
    }
    result = evaluate_expression(expression, "composite", series_data)

    assert result["value"].iloc[0] == 3.0
    assert result["value"].iloc[1] == 2.0
    assert result["value"].iloc[2] == 1.0


def test_composite_missing_op():
    """Test composite with missing operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [3.0, 4.0]}),
    }

    expression = {"operands": [{"series_code": "A"}, {"series_code": "B"}]}

    with pytest.raises(InvalidExpressionError, match="Missing operation"):
        evaluate_expression(expression, "composite", series_data)


def test_composite_invalid_op():
    """Test composite with invalid operation."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [3.0, 4.0]}),
    }

    expression = {"op": "invalid_op", "operands": [{"series_code": "A"}, {"series_code": "B"}]}

    with pytest.raises(InvalidExpressionError):
        evaluate_expression(expression, "composite", series_data)


def test_composite_insufficient_operands():
    """Test composite with less than 2 operands."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
    }

    expression = {"op": "sum", "operands": [{"series_code": "A"}]}

    with pytest.raises(InvalidExpressionError, match="at least 2 operands"):
        evaluate_expression(expression, "composite", series_data)


def test_nested_expression_series_math():
    """Test nested expression with series_math."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [10.0, 20.0, 30.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [2.0, 4.0, 6.0]}),
        "C": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [1.0, 2.0, 3.0]}),
    }

    expression = {
        "op": "add",
        "left": {"op": "multiply", "left": {"series_code": "A"}, "right": {"series_code": "B"}},
        "right": {"series_code": "C"},
    }
    result = evaluate_expression(expression, "series_math", series_data)

    # (10*2) + 1 = 21, (20*4) + 2 = 82, (30*6) + 3 = 183
    assert result["value"].iloc[0] == 21.0
    assert result["value"].iloc[1] == 82.0
    assert result["value"].iloc[2] == 183.0


def test_nested_expression_window_op():
    """Test nested expression with window_op."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=5), "value": [1.0, 2.0, 3.0, 4.0, 5.0]}),
    }

    expression = {
        "op": "sma",
        "series": {"op": "lag", "series": {"series_code": "A"}, "window": 1},
        "window": 2,
    }
    result = evaluate_expression(expression, "window_op", series_data)

    assert len(result) == 5


def test_nested_expression_composite():
    """Test nested expression with composite."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [1.0, 2.0, 3.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [4.0, 5.0, 6.0]}),
        "C": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3), "value": [7.0, 8.0, 9.0]}),
    }

    # Use "avg" instead of "sum" to avoid ambiguity with window_op
    expression = {
        "op": "add",
        "left": {
            "op": "avg",
            "operands": [{"series_code": "A"}, {"series_code": "B"}],
        },
        "right": {"series_code": "C"},
    }
    result = evaluate_expression(expression, "series_math", series_data)

    # ((1+4)/2) + 7 = 9.5, ((2+5)/2) + 8 = 11.5, ((3+6)/2) + 9 = 13.5
    assert result["value"].iloc[0] == 9.5
    assert result["value"].iloc[1] == 11.5
    assert result["value"].iloc[2] == 13.5


def test_missing_operand():
    """Test missing operand error."""
    series_data = {"A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]})}

    expression = {"op": "add", "left": {"series_code": "A"}}

    with pytest.raises(InvalidExpressionError, match="Missing operand"):
        evaluate_expression(expression, "series_math", series_data)


def test_series_alignment_different_times():
    """Test series alignment with different timestamps."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=3, freq="D"), "value": [1.0, 2.0, 3.0]}),
        "B": pd.DataFrame(
            {"obs_time": pd.date_range("2024-01-02", periods=3, freq="D"), "value": [4.0, 5.0, 6.0]}
        ),  # Offset by 1 day
    }

    expression = {"op": "add", "left": {"series_code": "A"}, "right": {"series_code": "B"}}
    result = evaluate_expression(expression, "series_math", series_data)

    # Should align on common timestamps
    assert len(result) >= 2


def test_unknown_operation_in_operand():
    """Test unknown operation in nested operand."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
    }

    expression = {
        "op": "add",
        "left": {"series_code": "A"},
        "right": {"op": "unknown_operation", "series": {"series_code": "A"}},
    }

    with pytest.raises(InvalidExpressionError, match="Unknown operation"):
        evaluate_expression(expression, "series_math", series_data)


def test_resolve_operand_missing_op():
    """Test _resolve_operand with missing op in nested expression."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
    }

    expression = {
        "op": "add",
        "left": {"series_code": "A"},
        "right": {"series": {"series_code": "A"}},  # Missing "op" key
    }

    with pytest.raises(InvalidExpressionError, match="Cannot resolve operand"):
        evaluate_expression(expression, "series_math", series_data)


def test_resolve_operand_empty_op():
    """Test _resolve_operand with empty op string."""
    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
    }

    expression = {
        "op": "add",
        "left": {"series_code": "A"},
        "right": {"op": ""},  # Empty op
    }

    with pytest.raises(InvalidExpressionError, match="Missing operation"):
        evaluate_expression(expression, "series_math", series_data)


def test_composite_empty_series_list():
    """Test composite with empty series list (edge case)."""
    # This tests the _align_multiple_series function with empty list
    # This shouldn't happen in practice but we test it for coverage
    from metrics_worker.application.services.expression_eval import _align_multiple_series

    with pytest.raises(InvalidExpressionError, match="Empty series list"):
        _align_multiple_series([])


def test_expression_type_enum():
    """Test evaluate_expression with ExpressionType enum directly."""
    from metrics_worker.domain.enums import ExpressionType

    series_data = {
        "A": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [1.0, 2.0]}),
        "B": pd.DataFrame({"obs_time": pd.date_range("2024-01-01", periods=2), "value": [3.0, 4.0]}),
    }

    expression = {"op": "add", "left": {"series_code": "A"}, "right": {"series_code": "B"}}
    result = evaluate_expression(expression, ExpressionType.SERIES_MATH, series_data)

    assert result["value"].iloc[0] == 4.0
    assert result["value"].iloc[1] == 6.0

