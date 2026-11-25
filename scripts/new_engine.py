#!/usr/bin/env python
"""Scaffold a new engine: `python scripts/new_engine.py NAME [--force]`.

See dq/sdk/scaffold.py for what is generated and what is left to fill in.
"""

import argparse
import logging
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from dq.sdk.scaffold import (
    NEXT_STEPS,
    ScaffoldError,
    class_name,
    scaffold,
)  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("name", help="Engine name: a lowercase identifier")
    parser.add_argument(
        "--root", type=Path, default=REPOSITORY_ROOT, help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--force", action="store_true", help="Overwrite generated files"
    )
    args = parser.parse_args(argv)
    try:
        scaffold(args.root, args.name, force=args.force)
    except ScaffoldError as error:
        print(f"Could not scaffold {args.name!r}: {error}", file=sys.stderr)
        return 1
    print(NEXT_STEPS.format(name=args.name, Class=class_name(args.name)))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
