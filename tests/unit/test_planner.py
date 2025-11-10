"""Unit tests for planner service."""

import pytest

from metrics_worker.application.services.planner import ReadPlan, plan_reads


def test_read_plan_add_series():
    """Test adding series to read plan."""
    plan = ReadPlan()
    plan.add_series("dataset1", "series1")
    plan.add_series("dataset1", "series2")
    plan.add_series("dataset2", "series3")

    assert plan.get_series_codes("dataset1") == ["series1", "series2"]
    assert plan.get_series_codes("dataset2") == ["series3"]
    assert plan.get_series_codes("dataset3") == []


def test_read_plan_columns():
    """Test read plan columns."""
    plan = ReadPlan()
    assert "obs_time" in plan.columns
    assert "value" in plan.columns
    assert "internal_series_code" in plan.columns


def test_plan_reads_simple():
    """Test planning reads for simple inputs."""
    expression = {"op": "add", "left": {"series_code": "A"}, "right": {"series_code": "B"}}
    expression_type = "series_math"
    inputs = [
        {"datasetId": "ds1", "seriesCode": "A"},
        {"datasetId": "ds1", "seriesCode": "B"},
    ]

    plan = plan_reads(expression, expression_type, inputs)

    assert "ds1" in plan.series_by_dataset
    assert "A" in plan.series_by_dataset["ds1"]
    assert "B" in plan.series_by_dataset["ds1"]


def test_plan_reads_multiple_datasets():
    """Test planning reads for multiple datasets."""
    expression = {"op": "add", "left": {"series_code": "A"}, "right": {"series_code": "B"}}
    expression_type = "series_math"
    inputs = [
        {"datasetId": "ds1", "seriesCode": "A"},
        {"datasetId": "ds2", "seriesCode": "B"},
    ]

    plan = plan_reads(expression, expression_type, inputs)

    assert "ds1" in plan.series_by_dataset
    assert "ds2" in plan.series_by_dataset
    assert "A" in plan.series_by_dataset["ds1"]
    assert "B" in plan.series_by_dataset["ds2"]


def test_plan_reads_window_op():
    """Test planning reads for window operation."""
    expression = {"op": "sma", "series": {"series_code": "A"}, "window": 5}
    expression_type = "window_op"
    inputs = [{"datasetId": "ds1", "seriesCode": "A"}]

    plan = plan_reads(expression, expression_type, inputs)

    assert "ds1" in plan.series_by_dataset
    assert "A" in plan.series_by_dataset["ds1"]


def test_plan_reads_composite():
    """Test planning reads for composite operation."""
    expression = {
        "op": "sum",
        "operands": [{"series_code": "A"}, {"series_code": "B"}, {"series_code": "C"}],
    }
    expression_type = "composite"
    inputs = [
        {"datasetId": "ds1", "seriesCode": "A"},
        {"datasetId": "ds1", "seriesCode": "B"},
        {"datasetId": "ds1", "seriesCode": "C"},
    ]

    plan = plan_reads(expression, expression_type, inputs)

    assert "ds1" in plan.series_by_dataset
    assert "A" in plan.series_by_dataset["ds1"]
    assert "B" in plan.series_by_dataset["ds1"]
    assert "C" in plan.series_by_dataset["ds1"]


def test_plan_reads_nested_expression():
    """Test planning reads for nested expression."""
    expression = {
        "op": "add",
        "left": {"op": "sma", "series": {"series_code": "A"}, "window": 5},
        "right": {"series_code": "B"},
    }
    expression_type = "series_math"
    inputs = [
        {"datasetId": "ds1", "seriesCode": "A"},
        {"datasetId": "ds1", "seriesCode": "B"},
    ]

    plan = plan_reads(expression, expression_type, inputs)

    assert "ds1" in plan.series_by_dataset
    assert "A" in plan.series_by_dataset["ds1"]
    assert "B" in plan.series_by_dataset["ds1"]

