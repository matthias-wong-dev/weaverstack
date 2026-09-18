"""The CLI owns its output encoding, so reports can carry their symbols."""

from __future__ import annotations

import io
import sys

from support.weaver_test import weaver_test

import weaver_cli
from weaver_cli.status import status_symbol

#: Every symbol normal CLI presentation writes. None encodes in cp1252.
SYMBOLS = "✓✗→·⋯"


def _code_page_stream() -> io.TextIOWrapper:
    return io.TextIOWrapper(io.BytesIO(), encoding="cp1252")


@weaver_test()
def test_a_windows_code_page_stream_carries_report_symbols(monkeypatch):
    out = _code_page_stream()
    err = _code_page_stream()
    monkeypatch.setattr(sys, "stdout", out)
    monkeypatch.setattr(sys, "stderr", err)

    assert weaver_cli.main([]) == 0

    print(status_symbol("succeeded"), SYMBOLS, file=sys.stdout)
    print(SYMBOLS, file=sys.stderr)
    out.flush()
    err.flush()

    assert f"✓ {SYMBOLS}" in out.buffer.getvalue().decode("utf-8")
    assert SYMBOLS in err.buffer.getvalue().decode("utf-8")
