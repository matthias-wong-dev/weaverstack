"""How a Livy submission reports itself, a claim about the transport.

Neither body imports Weaver, and both would hold against an Environment that had
never heard of it. They sat with the wheel tests only because of a filename.

One call each, and they cannot be merged: what each asserts is a property of the
submission itself, and a body that raised would take any other evidence in the
same payload down with it.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test


@weaver_test(remote=True)
def test_a_failing_statement_reports_its_error(livy_session):
    from weaver.fabric import LivyError

    with pytest.raises(LivyError, match="ZeroDivisionError|division"):
        livy_session.run("1 / 0\n")


@weaver_test(remote=True)
def test_printed_output_is_not_mistaken_for_a_result(livy_session):
    result = livy_session.run("print('just logging')\n")
    assert result.returned is False
    assert "just logging" in result.text
