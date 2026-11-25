# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Gate-event notifier port (ADR-008).

Separate from the sink port: a sink persists evidence; a notifier announces
gate events. Notification failures are logged and swallowed -- they must
never alter a run's outcome or block evidence durability.

HTTP adapters (webhook, Slack, Teams) share one delivery policy: up to three
attempts with capped exponential backoff, then a logged warning. Delivery
is at-most-once; consumers that need more subscribe to the durable evidence
store.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional
from urllib.parse import urlparse

from dq.exceptions import ConfigurationError
from dq.gate import severity_split

logger = logging.getLogger("dq.gate_events")

#: Delivery policy shared by the HTTP adapters (ADR-008, SPEC-002).
DEFAULT_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 30.0
DEFAULT_TIMEOUT_SECONDS = 10

#: Schemes an HTTP adapter will post to.
_ALLOWED_URL_SCHEMES = ("http", "https")


@dataclass(frozen=True, slots=True)
class GateEvent:
    """Immutable record of a gate result, built from validated outcomes.

    ``status``/``failed`` cover every failed check (evidence semantics).
    ``blocked``/``warned`` add the severity split: failures whose reported
    ``details.criticality`` is ``"warn"`` are advisory, everything else
    (including engines that report no severity) blocks.
    """

    run_key: int
    dataset: str
    status: str
    failed: tuple[str, ...]
    total: int
    ts: int
    job_id: str
    blocked: tuple[str, ...] = ()
    warned: tuple[str, ...] = ()
    report: Optional[str] = None

    @classmethod
    def from_outcomes(
        cls,
        run_key: int,
        dataset: str,
        outcomes: list[dict[str, Any]],
        ts: int,
        job_id: str,
        report: Optional[str] = None,
    ) -> GateEvent:
        """Build one event from a run's outcomes.

        Args:
            report: Optional reference to the run's ``dq-report/v1``
                document, when one was written.
        """
        failed = tuple(
            outcome.get("check", "unnamed")
            for outcome in outcomes
            if outcome.get("success") is not True
        )
        blocked, warned = severity_split(outcomes)
        return cls(
            run_key=run_key,
            dataset=dataset,
            status="passed" if not failed else "failed",
            failed=failed,
            total=len(outcomes),
            ts=ts,
            job_id=job_id,
            blocked=blocked,
            warned=warned,
            report=report,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_key": self.run_key,
            "dataset": self.dataset,
            "status": self.status,
            "failed": list(self.failed),
            "total": self.total,
            "ts": self.ts,
            "job_id": self.job_id,
            "blocked": list(self.blocked),
            "warned": list(self.warned),
            "report": self.report,
        }

    def summary_lines(self) -> list[str]:
        """Human-readable lines shared by the chat adapters."""
        lines = [
            f"{len(self.failed)} of {self.total} checks failed: "
            f"{len(self.blocked)} blocking, {len(self.warned)} warning.",
            f"run_key={self.run_key} job_id={self.job_id} ts={self.ts}",
        ]
        if self.failed:
            lines.append("Failed: " + ", ".join(self.failed[:20]))
        if self.report:
            lines.append(f"Report: {self.report}")
        return lines


class GateNotifier(ABC):
    """Port for announcing gate events to humans or downstream systems.

    Implementations must not raise: notification failures are logged and
    swallowed so they never alter a run's outcome.
    """

    @abstractmethod
    def notify(self, event: GateEvent) -> None:
        """Announce one gate event. Must not raise on delivery failure."""
        raise NotImplementedError


class LoggingNotifier(GateNotifier):
    """Default notifier: structured log line, always succeeds."""

    def __init__(self):
        self._logger = logging.getLogger("dq.gate_events")

    def notify(self, event: GateEvent) -> None:
        self._logger.info(
            "gate_event dataset=%s status=%s run_key=%d failed=%d/%d blocked=%d warned=%d",
            event.dataset,
            event.status,
            event.run_key,
            len(event.failed),
            event.total,
            len(event.blocked),
            len(event.warned),
        )


class InMemoryNotifier(GateNotifier):
    """Recording notifier for tests and embedders; never fails."""

    def __init__(self):
        self.events: list[GateEvent] = []

    def notify(self, event: GateEvent) -> None:
        self.events.append(event)


class CompositeNotifier(GateNotifier):
    """Fan-out to multiple notifiers; each failure is logged and swallowed."""

    def __init__(self, *notifiers: GateNotifier):
        self._notifiers = notifiers

    def notify(self, event: GateEvent) -> None:
        for notifier in self._notifiers:
            try:
                notifier.notify(event)
            except Exception as error:  # noqa: BLE001 - delivery is fail-open
                logger.warning(
                    "notifier %s failed for run_key=%d: %s",
                    type(notifier).__name__,
                    event.run_key,
                    error,
                )


class _HttpNotifier(GateNotifier):
    """JSON POST with retries; the base of the HTTP adapters."""

    #: Name used in log lines and configuration.
    type_name = "webhook"

    def __init__(
        self,
        url: str,
        *,
        attempts: int = DEFAULT_ATTEMPTS,
        backoff: float = DEFAULT_BACKOFF_SECONDS,
        max_backoff: float = MAX_BACKOFF_SECONDS,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if attempts < 1:
            raise ConfigurationError(
                f"{self.type_name} notifier requires at least one attempt, got {attempts}"
            )
        scheme = urlparse(url).scheme
        if scheme not in _ALLOWED_URL_SCHEMES:
            # The URL arrives through the environment; an allow-list keeps a
            # mis-set variable from pointing the HTTP adapter at a local file
            # or an unexpected scheme.
            raise ConfigurationError(
                f"{self.type_name} notifier URL must use http or https, "
                f"got {scheme or 'no scheme'!r}"
            )
        self._url = url
        self._attempts = attempts
        self._backoff = backoff
        self._max_backoff = max_backoff
        self._timeout = timeout
        self._sleep = sleep

    def payload(self, event: GateEvent) -> dict[str, Any]:
        """Return the JSON body for ``event``; adapters override this."""
        return event.to_dict()

    def notify(self, event: GateEvent) -> None:
        body = json.dumps(self.payload(event)).encode("utf-8")
        for attempt in range(1, self._attempts + 1):
            request = urllib.request.Request(
                self._url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                urllib.request.urlopen(request, timeout=self._timeout)
                return
            except Exception as error:  # noqa: BLE001 - delivery is fail-open
                if attempt == self._attempts:
                    logger.warning(
                        "%s delivery failed after %d attempt(s) for run_key=%d: %s",
                        self.type_name,
                        attempt,
                        event.run_key,
                        error,
                    )
                    return
                delay = min(self._backoff * 2 ** (attempt - 1), self._max_backoff)
                logger.warning(
                    "%s attempt %d/%d failed for run_key=%d: %s; retrying in %.1fs",
                    self.type_name,
                    attempt,
                    self._attempts,
                    event.run_key,
                    error,
                    delay,
                )
                self._sleep(delay)


class WebhookNotifier(_HttpNotifier):
    """JSON POST of the gate event; fail-open with logged errors."""

    type_name = "webhook"


class SlackNotifier(_HttpNotifier):
    """Slack incoming-webhook adapter (message attachment with fields)."""

    type_name = "slack"

    def payload(self, event: GateEvent) -> dict[str, Any]:
        if event.blocked:
            color = "danger"
        elif event.warned:
            color = "warning"
        else:
            color = "good"
        fields = [
            {
                "title": "Checks",
                "value": (
                    f"{len(event.failed)} of {event.total} failed "
                    f"({len(event.blocked)} blocking, {len(event.warned)} warning)"
                ),
                "short": False,
            },
            {
                "title": "Run",
                "value": f"run_key={event.run_key} job_id={event.job_id}",
                "short": False,
            },
        ]
        if event.failed:
            fields.append(
                {
                    "title": "Failed",
                    "value": ", ".join(event.failed[:20]),
                    "short": False,
                }
            )
        if event.report:
            fields.append({"title": "Report", "value": event.report, "short": False})
        return {
            "text": f"Data quality gate {event.status}: {event.dataset}",
            "attachments": [{"color": color, "fields": fields}],
        }


class TeamsNotifier(_HttpNotifier):
    """Microsoft Teams incoming-webhook adapter (Adaptive Card)."""

    type_name = "teams"

    def payload(self, event: GateEvent) -> dict[str, Any]:
        emphasis = "attention" if event.blocked else "default"
        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": f"Data quality gate {event.status}: {event.dataset}",
                                "weight": "Bigger",
                                "wrap": True,
                            },
                            {
                                "type": "TextBlock",
                                "text": "\n".join(event.summary_lines()),
                                "wrap": True,
                                "emphasis": emphasis,
                            },
                        ],
                    },
                }
            ],
        }


def _url_from_env(kind: str, entry: dict) -> str:
    url_env = entry.get("url_env", None)
    if not url_env:
        raise ConfigurationError(
            f"{kind} notifier requires a 'url_env' key naming the "
            "environment variable that holds the webhook URL"
        )
    url = os.environ.get(url_env, None)
    if not url:
        raise ConfigurationError(
            f"environment variable {url_env!r} must contain the {kind} webhook URL"
        )
    return url


def notifier_from_config(
    notifier_config, *, sleep: Callable[[float], None] = time.sleep
) -> GateNotifier:
    """Build a notifier from configuration (fail closed on unknown types).

    Args:
        notifier_config: The ``dqframework.notifiers`` list; empty means the
            default logging notifier.
        sleep: Backoff sleep, injectable so retry tests never wait.
    """
    http_types = {
        "webhook": WebhookNotifier,
        "slack": SlackNotifier,
        "teams": TeamsNotifier,
    }
    if not notifier_config:
        return LoggingNotifier()
    notifiers: list[GateNotifier] = []
    for entry in notifier_config:
        kind = entry.get("type", None) if isinstance(entry, dict) else None
        if kind == "logging":
            notifiers.append(LoggingNotifier())
        elif kind == "memory":
            notifiers.append(InMemoryNotifier())
        elif kind in http_types:
            adapter = http_types[kind]
            notifiers.append(adapter(_url_from_env(kind, entry), sleep=sleep))
        else:
            raise ConfigurationError(
                f"notifier type {kind!r} is not supported; use 'logging', 'memory', "
                "'webhook', 'slack' or 'teams'"
            )
    if len(notifiers) == 1:
        return notifiers[0]
    return CompositeNotifier(*notifiers)
