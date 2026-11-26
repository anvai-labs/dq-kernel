# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Gate-event notifier contracts (ADR-008)."""

from unittest.mock import MagicMock, patch

import pytest

from dq.exceptions import ConfigurationError
from dq.notifier import (
    CompositeNotifier,
    GateEvent,
    GateNotifier,
    LoggingNotifier,
    WebhookNotifier,
    notifier_from_config,
)


def make_event(**overrides):
    defaults = {
        "run_key": 42,
        "dataset": "bars",
        "status": "passed",
        "failed": (),
        "total": 3,
        "ts": 1700000000000,
        "job_id": "local-1234",
    }
    defaults.update(overrides)
    return GateEvent(**defaults)


def test_gate_event_from_outcomes_collects_failures():
    outcomes = [
        {"check": "complete", "success": True},
        {"check": "unique", "success": False},
        {"check": "size", "success": True},
        {"check": "range", "success": False},
    ]
    event = GateEvent.from_outcomes(42, "bars", outcomes, 1700000000000, "job-1")
    assert event.status == "failed"
    assert event.failed == ("unique", "range")
    assert event.total == 4


def test_gate_event_passing_run_has_empty_failures():
    outcomes = [{"check": "complete", "success": True}]
    event = GateEvent.from_outcomes(42, "bars", outcomes, 0, "job-1")
    assert event.status == "passed"
    assert event.failed == ()


def test_logging_notifier_produces_structured_output():
    notifier = LoggingNotifier()
    event = make_event()
    notifier.notify(event)


def test_webhook_notifier_posts_json():
    event = make_event()
    with patch("urllib.request.urlopen") as mock_urlopen:
        notifier = WebhookNotifier("https://hooks.example.com/dq")
        notifier.notify(event)
        mock_urlopen.assert_called_once()
        request = mock_urlopen.call_args[0][0]
        assert request.get_method() == "POST"
        assert request.headers["Content-type"] == "application/json"


def test_webhook_notifier_swallows_delivery_failures():
    event = make_event()
    with patch("urllib.request.urlopen", side_effect=OSError("network down")):
        notifier = WebhookNotifier("https://hooks.example.com/unreachable")
        notifier.notify(event)


def test_webhook_notifier_swallows_http_errors():
    event = make_event()
    import urllib.error

    with patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError("HTTP 500"),
    ):
        notifier = WebhookNotifier("https://hooks.example.com/failing")
        notifier.notify(event)


def test_composite_notifier_fans_out_and_swallows_failures():
    good = LoggingNotifier()
    bad = WebhookNotifier("https://unreachable.example.com/dq")
    composite = CompositeNotifier(good, bad)
    event = make_event()
    with patch("urllib.request.urlopen", side_effect=OSError("down")):
        composite.notify(event)


def test_notifier_from_config_builds_logging_notifier():
    notifier = notifier_from_config([])
    assert isinstance(notifier, LoggingNotifier)


def test_notifier_from_config_builds_webhook_notifier(monkeypatch):
    monkeypatch.setenv("DQ_WEBHOOK_URL", "https://hooks.example.com/dq")
    notifier = notifier_from_config([{"type": "webhook", "url_env": "DQ_WEBHOOK_URL"}])
    assert isinstance(notifier, WebhookNotifier)


def test_notifier_from_config_fails_closed_without_webhook_url(monkeypatch):
    monkeypatch.delenv("DQ_WEBHOOK_URL", raising=False)
    with pytest.raises(ConfigurationError, match="DQ_WEBHOOK_URL"):
        notifier_from_config([{"type": "webhook", "url_env": "DQ_WEBHOOK_URL"}])


def test_notifier_from_config_rejects_unknown_types():
    with pytest.raises(ConfigurationError, match="not supported"):
        notifier_from_config([{"type": "sms"}])


# ------------------------------------------------------- severity split (v2)


def _outcome(check, success, criticality=None):
    details = {"criticality": criticality} if criticality else {}
    return {"check": check, "success": success, "details": details}


def test_gate_event_splits_failures_by_criticality():
    event = GateEvent.from_outcomes(
        run_key=1,
        dataset="d",
        outcomes=[
            _outcome("err_rule", False, "error"),
            _outcome("warn_rule", False, "warn"),
            _outcome("passing", True, "error"),
        ],
        ts=1,
        job_id="job",
    )
    assert event.status == "failed"
    assert event.failed == ("err_rule", "warn_rule")
    assert event.blocked == ("err_rule",)
    assert event.warned == ("warn_rule",)


def test_gate_event_absent_criticality_blocks_conservatively():
    event = GateEvent.from_outcomes(
        run_key=1,
        dataset="d",
        outcomes=[{"check": "legacy", "success": False, "details": {}}],
        ts=1,
        job_id="job",
    )
    assert event.blocked == ("legacy",)
    assert event.warned == ()


def test_gate_event_missing_details_key_blocks_conservatively():
    event = GateEvent.from_outcomes(
        run_key=1,
        dataset="d",
        outcomes=[{"check": "no_details", "success": False}],
        ts=1,
        job_id="job",
    )
    assert event.blocked == ("no_details",)
    assert event.warned == ()


def test_gate_event_severity_split_preserves_configuration_order():
    event = GateEvent.from_outcomes(
        run_key=1,
        dataset="d",
        outcomes=[
            _outcome("b_warn", False, "warn"),
            _outcome("a_err", False, "error"),
            _outcome("c_warn", False, "warn"),
        ],
        ts=1,
        job_id="job",
    )
    assert event.failed == ("b_warn", "a_err", "c_warn")
    assert event.warned == ("b_warn", "c_warn")
    assert event.blocked == ("a_err",)


def test_gate_event_to_dict_includes_severity_split():
    event = GateEvent.from_outcomes(
        run_key=1,
        dataset="d",
        outcomes=[_outcome("warn_rule", False, "warn")],
        ts=1,
        job_id="job",
    )
    payload = event.to_dict()
    assert payload["blocked"] == [] and payload["warned"] == ["warn_rule"]
    assert payload["status"] == "failed" and payload["failed"] == ["warn_rule"]


def test_gate_event_positional_construction_back_compat():
    event = GateEvent(1, "d", "failed", ("a",), 1, 1, "job")
    assert event.blocked == () and event.warned == ()
