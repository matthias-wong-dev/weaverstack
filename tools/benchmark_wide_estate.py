#!/usr/bin/env python3
"""Run the deterministic wide-estate benchmark."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))

from support.wide_estate_benchmark import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
