"""Unit tests for event parsing."""

import json

from metrics_worker.application.dto.events import MetricRunRequestedEvent


def test_parse_metric_run_requested_event():
    """Test parsing metric_run_requested event."""
    event_data = {
        "type": "metric_run_requested",
        "runId": "550e8400-e29b-41d4-a716-446655440000",
        "metricCode": "ratio.reserves_to_base",
        "expressionType": "series_math",
        "expressionJson": {
            "op": "ratio",
            "left": {"series_code": "BCRA_RESERVAS_USD_M_D"},
            "right": {"series_code": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D"},
            "scale": 1,
        },
        "inputs": [
            {
                "datasetId": "bcra_infomondia_series",
                "seriesCode": "BCRA_RESERVAS_USD_M_D",
            },
            {
                "datasetId": "bcra_infomondia_series",
                "seriesCode": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D",
            },
        ],
        "catalog": {
            "datasets": {
                "bcra_infomondia_series": {
                    "manifestPath": "bcra_infomondia_series/current/manifest.json",
                    "projectionsPath": "projections/bcra_infomondia_series",
                },
            },
        },
        "output": {
            "basePath": "s3://bucket-name/metrics/ratio.reserves_to_base/",
        },
    }

    event = MetricRunRequestedEvent(**event_data)

    assert event.type == "metric_run_requested"
    assert event.run_id == "550e8400-e29b-41d4-a716-446655440000"
    assert event.metric_code == "ratio.reserves_to_base"
    assert event.expression_type == "series_math"
    assert event.expression_json["op"] == "ratio"
    assert len(event.inputs) == 2


def test_parse_event_ignores_extra_fields():
    """Test parsing event ignores extra fields that may be present in SNS messages."""
    event_data = {
        "type": "metric_run_requested",
        "runId": "550e8400-e29b-41d4-a716-446655440000",
        "metricCode": "ratio.reserves_to_base",
        "expressionType": "series_math",
        "expressionJson": {
            "op": "ratio",
            "left": {"series_code": "A"},
            "right": {"series_code": "B"},
        },
        "inputs": [{"datasetId": "ds1", "seriesCode": "A"}],
        "catalog": {
            "datasets": {
                "ds1": {
                    "manifestPath": "ds1/current/manifest.json",
                    "projectionsPath": "projections/ds1",
                }
            }
        },
        "output": {"basePath": "s3://bucket/metrics/test/"},
    }

    # Pydantic will ignore extra fields by default, so this should still parse successfully
    event = MetricRunRequestedEvent(**event_data)

    assert event.type == "metric_run_requested"
    assert event.run_id == "550e8400-e29b-41d4-a716-446655440000"


def test_parse_sns_wrapped_message():
    """Test parsing SNS-wrapped message."""
    sns_body = {
        "Type": "Notification",
        "MessageId": "uuid-sns",
        "TopicArn": "arn:aws:sns:us-east-1:123:metric-run-request.fifo",
        "Message": json.dumps(
            {
                "type": "metric_run_requested",
                "runId": "550e8400-e29b-41d4-a716-446655440000",
                "metricCode": "ratio.reserves_to_base",
                "expressionType": "series_math",
                "expressionJson": {
                    "op": "ratio",
                    "left": {"series_code": "A"},
                    "right": {"series_code": "B"},
                },
                "inputs": [{"datasetId": "ds1", "seriesCode": "A"}],
                "catalog": {
                    "datasets": {
                        "ds1": {
                            "manifestPath": "ds1/current/manifest.json",
                            "projectionsPath": "projections/ds1",
                        }
                    }
                },
                "output": {"basePath": "s3://bucket/metrics/test/"},
            }
        ),
        "MessageAttributes": {
            "type": {"Type": "String", "Value": "metric_run_requested"},
            "metricCode": {"Type": "String", "Value": "ratio.reserves_to_base"},
        },
    }

    sns_message = json.loads(sns_body["Message"])
    message_attributes = sns_body.get("MessageAttributes", {})

    event_data = dict(sns_message)

    if message_attributes:
        type_attr = message_attributes.get("type", {}).get("Value")
        metric_code_attr = message_attributes.get("metricCode", {}).get("Value")

        if type_attr:
            event_data["type"] = type_attr
        if metric_code_attr:
            event_data["metricCode"] = metric_code_attr

    event = MetricRunRequestedEvent(**event_data)

    assert event.type == "metric_run_requested"
    assert event.metric_code == "ratio.reserves_to_base"


def test_window_op_expression():
    """Test window_op expression type."""
    event_data = {
        "type": "metric_run_requested",
        "runId": "660e8400-e29b-41d4-a716-446655440001",
        "metricCode": "mon.base_30d_sma",
        "expressionType": "window_op",
        "expressionJson": {
            "op": "sma",
            "series": {"series_code": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D"},
            "window": 30,
        },
        "inputs": [
            {
                "datasetId": "bcra_infomondia_series",
                "seriesCode": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D",
            }
        ],
        "catalog": {
            "datasets": {
                "bcra_infomondia_series": {
                    "manifestPath": "bcra_infomondia_series/current/manifest.json",
                    "projectionsPath": "projections/bcra_infomondia_series",
                },
            },
        },
        "output": {
            "basePath": "s3://my-bucket/metrics/mon.base_30d_sma/",
        },
    }

    event = MetricRunRequestedEvent(**event_data)

    assert event.expression_type == "window_op"
    assert event.expression_json["op"] == "sma"
    assert event.expression_json["window"] == 30


def test_composite_expression():
    """Test composite expression type."""
    event_data = {
        "type": "metric_run_requested",
        "runId": "770e8400-e29b-41d4-a716-446655440002",
        "metricCode": "mon.base_ampliada_ars",
        "expressionType": "composite",
        "expressionJson": {
            "op": "sum",
            "operands": [
                {"series_code": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D"},
                {"series_code": "BCRA_LELIQ_NOTALIQ_ARS_BN_D"},
                {"series_code": "BCRA_PASES_PASIVOS_ARS_BN_D"},
            ],
        },
        "inputs": [
            {
                "datasetId": "bcra_infomondia_series",
                "seriesCode": "BCRA_BASE_MONETARIA_TOTAL_ARS_BN_D",
            },
            {
                "datasetId": "bcra_infomondia_series",
                "seriesCode": "BCRA_LELIQ_NOTALIQ_ARS_BN_D",
            },
            {
                "datasetId": "bcra_infomondia_series",
                "seriesCode": "BCRA_PASES_PASIVOS_ARS_BN_D",
            },
        ],
        "catalog": {
            "datasets": {
                "bcra_infomondia_series": {
                    "manifestPath": "bcra_infomondia_series/current/manifest.json",
                    "projectionsPath": "projections/bcra_infomondia_series",
                },
            },
        },
        "output": {
            "basePath": "s3://my-bucket/metrics/mon.base_ampliada_ars/",
        },
    }

    event = MetricRunRequestedEvent(**event_data)

    assert event.expression_type == "composite"
    assert event.expression_json["op"] == "sum"
    assert len(event.expression_json["operands"]) == 3
