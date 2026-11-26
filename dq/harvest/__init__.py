# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Import other tools' quality results as this framework's evidence (SPEC-013).

    from pathlib import Path

    from dq.harvest import harvest

    outcomes = harvest(
        Path("target/run_results.json").read_bytes(),
        reference="target/run_results.json",
    )

Each outcome is an ordinary outcome dictionary -- ``check``, ``success`` and
bounded ``details`` -- carrying ``details.origin``, which says which system
produced it, under which reference, from which bytes. So imported results
reach the same report, gate and sinks as the framework's own:

    from dq.gate import evaluate_gate
    from dq.report import build_report, write_report

    decision = evaluate_gate(outcomes, "error")

Three source systems are read: dbt's ``run_results.json``, a Great
Expectations validation result, and a columnar file -- CSV, or a JSON array
of records -- that any tool can write. The system is told from the document,
or stated with ``system``.

A source that cannot be imported is refused with :class:`HarvestError`,
naming the record at fault. This package imports no vendor library: a
harvester parses bytes.
"""

from types import MappingProxyType
from typing import List, Optional

from dq.exceptions import HarvestError
from dq.harvest.base import Harvester, Imported
from dq.harvest.columnar import ColumnarHarvester
from dq.harvest.dbt import DbtRunResultsHarvester
from dq.harvest.greatexpectations import GreatExpectationsHarvester

#: The harvesters, in the order sources are told apart: a vendor's document
#: before the format that takes any list of records.
_HARVESTERS = (
    DbtRunResultsHarvester(),
    GreatExpectationsHarvester(),
    ColumnarHarvester(),
)

#: Every harvester by the name of its source system.
HARVESTERS = MappingProxyType(
    {harvester.system: harvester for harvester in _HARVESTERS}
)

__all__ = [
    "HARVESTERS",
    "HarvestError",
    "Harvester",
    "Imported",
    "detect",
    "harvest",
]


def detect(source: bytes) -> str:
    """Return the source system whose harvester reads ``source``.

    Raises:
        HarvestError: If no harvester recognises it.
    """
    for harvester in _HARVESTERS:
        if harvester.recognises(source):
            return harvester.system
    raise HarvestError(
        "The source is not recognised: it is not a dbt run_results.json, a "
        "Great Expectations validation result, or a columnar file. State the "
        "system it is with system=... to see why it is refused.",
        system=None,
    )


def harvest(
    source: bytes, *, reference: str, system: Optional[str] = None
) -> List[dict]:
    """Import a source's bytes as outcome dictionaries.

    Args:
        source: The source's bytes, read once: every outcome's provenance
            carries the digest of exactly these.
        reference: What the source is called in its provenance, as the
            caller wrote it.
        system: The source system: ``dbt``, ``greatexpectations`` or
            ``columnar``. Told from the document when omitted. Stating it
    skips detection, which matters when a document could be read two ways.

    Returns:
        One outcome dictionary per check, each with ``details.origin``.

    Raises:
        HarvestError: If the system is unknown, or the source cannot be
            imported -- it is not the document the harvester reads, a record
            is malformed, or two checks share a name.
    """
    if system is None:
        system = detect(source)
    harvester = HARVESTERS.get(system)
    if harvester is None:
        raise HarvestError(
            f"Unknown source system {system!r}: expected one of "
            f"{sorted(HARVESTERS)}",
            system=None,
        )
    return harvester.harvest(source, reference=reference)
