"""The Spark half of `weaver doctor`: does a session really start?

The rest of `tests/test_diagnostics.py` inspects what the report *says* and needs
no JVM. This one asserts the report agrees with reality, so it needs a session —
which is why it lives here rather than beside its pure-Python siblings.
"""

from __future__ import annotations

import pytest

from weaver.diagnostics import check_local_spark

pytestmark = pytest.mark.spark


def test_the_report_agrees_with_a_session_actually_starting(spark):
    assert check_local_spark().ok
    assert spark.range(3).count() == 3
