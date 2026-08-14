"""A Test reaches its artefact the way a load does, and its outcome is settled.

Moved here from the local Spark tier rather than deleted with it. The claim is
about *dispatch* — one import, one runtime context, a different engine call —
but a Test's deployed artefact returns a Spark frame and Weaver's comparison
reads it, so the claim cannot be made without a real Spark session. A double
returning a frame-shaped object would be this suite modelling Spark, which is
the one thing the test substrate must never do.

``hosted``, because the artefacts run as the installed wheel: the runtime
context and the comparison are :mod:`weaver.runtime.test_compare` inside the
Fabric session.

Three outcomes, chosen because each is settled by a different rule:

.. code-block:: text

    Agrees       both sides match, so nothing is missing and nothing is extra
    Disagrees    a discrepancy on both sides, reported with its counts
    Unreadable   could not be evaluated at all, which is not "found nothing"

The last is the one that matters most: the answer a validation must never give
is "found nothing" when what happened is that it could not look. Its Warehouse
twin is ``test_warehouse_validation_primitive``; if the two disagree, one set of
validation semantics has become two.
"""

from __future__ import annotations

import pytest

pytestmark = [
    pytest.mark.fabric,
    pytest.mark.hosted,
]


#: A Test's deployed artefact is read rather than called, and what it returns is
#: a real Spark frame of sides. Two literal rows are enough to settle each
#: outcome: what a Test *means* is proven by the comparison's own tests, and
#: what is proven here is that the run reaches it and settles what comes back.
VALIDATIONS = {
    "Agrees": '''\
from pyspark.sql.types import StringType, StructField, StructType

SIDES = StructType([StructField("_weaver_side", StringType())])


class {name}:
    """Both sides agree: no rows, so nothing missing and nothing unexpected."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def read(self):
        return self.spark.createDataFrame([], SIDES)
''',
    "Disagrees": '''\
from pyspark.sql.types import StringType, StructField, StructType

SIDES = StructType([StructField("_weaver_side", StringType())])


class {name}:
    """One row expected and never seen, one seen and never expected."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def read(self):
        return self.spark.createDataFrame(
            [("expected",), ("actual",), ("actual",)], SIDES
        )
''',
    "Unreadable": '''\
class {name}:
    """Cannot be evaluated at all — which is not the same as finding nothing."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def read(self):
        raise RuntimeError("the table this Test reads does not exist")
''',
}


@pytest.mark.skip(
    reason=(
        "moved from the deleted local Spark tier and not yet wired to the "
        "Fabric harness — see Milestone 1 in the Fabric-only runtime PR"
    )
)
def test_a_validation_reaches_its_artefact_the_same_way_a_load_does():
    """The same import, the same runtime context, a different engine call."""


@pytest.mark.skip(reason="see above")
def test_a_disagreement_is_a_failure_carrying_what_it_found():
    """One missing and two unexpected, reported rather than summarised away."""


@pytest.mark.skip(reason="see above")
def test_a_validation_that_could_not_run_is_invalid_rather_than_failed():
    """The one answer a validation must never give is "found nothing"."""
