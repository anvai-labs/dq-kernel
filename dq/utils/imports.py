# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Optional-dependency import helper shared by every vendor-wrapping component.

Every engine, and the OpenTelemetry observer, that wraps an optional vendor
library uses the same fail-closed pattern: import lazily, and on absence
raise an ImportError that names the extra, the package, the poetry extra,
the pinned version range, and any known caveat. Centralizing the message
keeps install hints consistent (and test-asserted) across components.
"""

import importlib
from typing import Optional


def require_extra(
    module: str,
    package: str,
    extra: str,
    pin: Optional[str] = None,
    note: Optional[str] = None,
    kind: str = "engine",
):
    """Import an optional vendor module or raise with install instructions.

    Args:
        module: Dotted module path to import (returned on success).
        package: Distribution name as it appears on PyPI.
        extra: Poetry/pip extra that provides the package; also the
            engine-facing name used in the error message.
        pin: Version constraint shown in the pip hint (defaults to the
            bare package name).
        note: Optional trailing caveat (e.g. Python-version or
            extra-conflict constraints).
        kind: What the extra is for, as the error message names it: an
            ``engine`` (the default), an ``observer``, an ``adapter``, a
            ``runner`` or a ``sink``. Say what the caller is: a sink that
            called itself an engine sent its reader to the wrong page.

    Returns:
        The imported module.

    Raises:
        ImportError: Naming the extra, package, pin, and note, chained
            from the original import failure.
    """
    try:
        return importlib.import_module(module)
    except ImportError as exc:
        message = (
            f"The {extra} {kind} requires the optional {package} package; "
            f"install it with `poetry install -E {extra}` (or "
            f"`pip install '{pin or package}'`)."
        )
        if note:
            message += f" Note: {note}"
        raise ImportError(message) from exc
