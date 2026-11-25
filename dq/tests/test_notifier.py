# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Gate-event notifier contracts (ADR-008)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from dq.exceptions import ConfigurationError
from dq.notifier import (
    CompositeNotifier,
    GateEvent,
    GateNotifier,
    InMemoryNotifier,
    LoggingNotifier,
    SlackNotifier,
    TeamsNotifier,
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
        notifier = WebhookNotifier(
            "https://hooks.example.com/unreachable", sleep=lambda _seconds: None
        )
        notifier.notify(event)


def test_webhook_notifier_swallows_http_errors():
    event = make_event()
    import urllib.error

    with patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.URLError("HTTP 500"),
    ):
        notifier = WebhookNotifier(
            "https://hooks.example.com/failing", sleep=lambda _s: None
        )
        notifier.notify(event)


def test_composite_notifier_fans_out_and_swallows_failures():
    good = LoggingNotifier()
    bad = WebhookNotifier(
        "https://unreachable.example.com/dq", sleep=lambda _seconds: None
    )
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


# ------------------------------------------------ retry with backoff (G3)


NO_SLEEP = lambda _seconds: None  # noqa: E731


def flaky_urlopen(*results):
    """Return (mock, calls) where each call yields the next result."""
    calls = []

    def fake(request, timeout=None):
        calls.append(request)
        outcome = results[min(len(calls), len(results)) - 1]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    return fake, calls


def test_webhook_retries_three_times_with_backoff_then_gives_up():
    sleeps = []
    fake, calls = flaky_urlopen(OSError("network down"))
    with patch("urllib.request.urlopen", side_effect=fake):
        WebhookNotifier("https://hooks.example.com/dq", sleep=sleeps.append).notify(
            make_event()
        )
    assert len(calls) == 3
    assert sleeps == [1.0, 2.0]  # capped exponential backoff between attempts


def test_webhook_stops_retrying_once_a_later_attempt_succeeds():
    sleeps = []
    fake, calls = flaky_urlopen(OSError("down"), OSError("down"), object())
    with patch("urllib.request.urlopen", side_effect=fake):
        WebhookNotifier("https://hooks.example.com/dq", sleep=sleeps.append).notify(
            make_event()
        )
    assert len(calls) == 3
    assert sleeps == [1.0, 2.0]

    sleeps = []
    fake, calls = flaky_urlopen(OSError("down"), object())
    with patch("urllib.request.urlopen", side_effect=fake):
        WebhookNotifier("https://hooks.example.com/dq", sleep=sleeps.append).notify(
            make_event()
        )
    assert len(calls) == 2
    assert sleeps == [1.0]


def test_webhook_backoff_is_capped():
    sleeps = []
    fake, _ = flaky_urlopen(OSError("down"))
    with patch("urllib.request.urlopen", side_effect=fake):
        WebhookNotifier(
            "https://hooks.example.com/dq", backoff=20.0, sleep=sleeps.append
        ).notify(make_event())
    assert sleeps == [20.0, 30.0]


def test_webhook_attempt_count_is_configurable_and_must_be_positive():
    fake, calls = flaky_urlopen(OSError("down"))
    with patch("urllib.request.urlopen", side_effect=fake):
        WebhookNotifier(
            "https://hooks.example.com/dq", attempts=1, sleep=NO_SLEEP
        ).notify(make_event())
    assert len(calls) == 1
    with pytest.raises(ConfigurationError, match="at least one attempt"):
        WebhookNotifier("https://hooks.example.com/dq", attempts=0)


# ------------------------------------------- chat adapters and memory (G5)


def _posted_payload(adapter, event):
    """Post ``event`` through ``adapter`` against a fake transport."""
    with patch("urllib.request.urlopen") as mock_urlopen:
        adapter.notify(event)
    request = mock_urlopen.call_args[0][0]
    assert request.get_method() == "POST"
    assert request.headers["Content-type"] == "application/json"
    return json.loads(request.data.decode("utf-8"))


RICH = dict(
    run_key=7,
    dataset="orders",
    status="failed",
    failed=("broken", "stale"),
    total=5,
    ts=7,
    job_id="job-1",
    blocked=("broken",),
    warned=("stale",),
    report="s3://evidence/run-7.json",
)


@pytest.mark.parametrize(
    ("failed", "blocked", "warned", "color"),
    [
        (("broken",), ("broken",), (), "danger"),
        (("stale",), (), ("stale",), "warning"),
        ((), (), (), "good"),
    ],
    ids=["blocking", "advisory-only", "passed"],
)
def test_slack_payload_is_shaped_from_the_event(failed, blocked, warned, color):
    event = make_event(
        **{
            **RICH,
            "failed": failed,
            "blocked": blocked,
            "warned": warned,
            "status": "failed" if failed else "passed",
        }
    )
    payload = _posted_payload(
        SlackNotifier("https://hooks.slack.com/x", sleep=NO_SLEEP), event
    )
    assert payload["attachments"][0]["color"] == color
    fields = {
        field["title"]: field["value"] for field in payload["attachments"][0]["fields"]
    }
    assert fields["Report"] == "s3://evidence/run-7.json"
    assert fields["Run"] == "run_key=7 job_id=job-1"
    assert payload["text"] == f"Data quality gate {event.status}: orders"


def test_teams_payload_is_an_adaptive_card_with_the_summary():
    payload = _posted_payload(
        TeamsNotifier("https://outlook.office.com/webhook/x", sleep=NO_SLEEP),
        make_event(**RICH),
    )
    attachment = payload["attachments"][0]
    assert attachment["contentType"] == "application/vnd.microsoft.card.adaptive"
    assert payload["type"] == "message"
    text = " ".join(block["text"] for block in attachment["content"]["body"])
    assert "Data quality gate failed: orders" in text
    assert "1 blocking, 1 warning" in text
    assert "s3://evidence/run-7.json" in text


@pytest.mark.parametrize(
    "adapter",
    [
        SlackNotifier("https://hooks.slack.com/x", sleep=NO_SLEEP),
        TeamsNotifier("https://outlook.office.com/webhook/x", sleep=NO_SLEEP),
    ],
    ids=["slack", "teams"],
)
def test_chat_adapters_are_fail_open(adapter):
    with patch("urllib.request.urlopen", side_effect=OSError("network down")):
        adapter.notify(make_event(**RICH))  # three attempts, then swallowed


def test_memory_notifier_records_events():
    recorder = InMemoryNotifier()
    event = make_event()
    recorder.notify(event)
    recorder.notify(event)
    assert recorder.events == [event, event]


def test_notifier_from_config_builds_memory_slack_and_teams(monkeypatch):
    monkeypatch.setenv("SLACK_URL", "https://hooks.slack.com/s")
    monkeypatch.setenv("TEAMS_URL", "https://outlook.office.com/t")
    notifier = notifier_from_config(
        [
            {"type": "memory"},
            {"type": "slack", "url_env": "SLACK_URL"},
            {"type": "teams", "url_env": "TEAMS_URL"},
        ],
        sleep=NO_SLEEP,
    )
    assert isinstance(notifier, CompositeNotifier)
    kinds = [type(n).__name__ for n in notifier._notifiers]
    assert kinds == ["InMemoryNotifier", "SlackNotifier", "TeamsNotifier"]


@pytest.mark.parametrize("kind", ["slack", "teams"])
def test_chat_notifiers_require_a_url_env_naming_a_set_variable(monkeypatch, kind):
    monkeypatch.delenv("UNSET_URL", raising=False)
    with pytest.raises(ConfigurationError, match="url_env"):
        notifier_from_config([{"type": kind}], sleep=NO_SLEEP)
    with pytest.raises(ConfigurationError, match="UNSET_URL"):
        notifier_from_config([{"type": kind, "url_env": "UNSET_URL"}], sleep=NO_SLEEP)


def test_unknown_notifier_types_list_the_supported_ones():
    with pytest.raises(ConfigurationError, match="'slack' or 'teams'"):
        notifier_from_config([{"type": "carrier-pigeon"}], sleep=NO_SLEEP)


def test_gate_event_json_carries_the_report_reference():
    assert make_event().to_dict()["report"] is None
    assert make_event(**RICH).to_dict()["report"] == "s3://evidence/run-7.json"
