"""What Spark does with the projection ``Table.dataframe()`` asks for.

The core suite decides which names a projection names. Only a real session and
a real Delta table settle what Spark then does with them: that an identifier
Spark would otherwise misparse resolves to the column it means, that a column
the table does not carry is refused rather than filled in, and that a business
column the author completed explicitly is accepted.

One Livy submission against the session the suite already holds. The probe
table is written by this test and overwritten each run, so nothing depends on
what an earlier module left behind.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

#: A declared schema chosen for what Spark finds hard: a name with a space, a
#: name containing a dot, and one ordinary name. The installed table carries an
#: undeclared column beside them, so down-selection is observable.
PROBE = '''
from pyspark.sql import functions as F

from weaver import Table
from weaver.declaration.metadata import PYTHON, parse_document

DOCUMENT = parse_document(
    """
Table ID: Sales.ProjectionProbe

Description: One row per order.

Lineage: The pytest suite writes it.

Primary key: Order Id

Schema:
  Order Id: string
  Order.Date: string
  Amount: string
""",
    language=PYTHON,
)


class Sales__ProjectionProbe(Table):
    def _document(self):
        return DOCUMENT

    def read(self):
        return []


probe = Sales__ProjectionProbe(spark)
path = probe.lakehouse.table_path(*probe.identity)

installed = spark.createDataFrame(
    [("o-1", "2026-04-23", "10.50", "ignored", "i", "u", "d", "sig")],
    schema=(
        "`Order Id` string, `Order.Date` string, Amount string, "
        "`Extra Undeclared` string, row_insert_datetime string, "
        "row_update_datetime string, row_delete_datetime string, "
        "row_signature string"
    ),
)
# Column mapping, because Delta refuses a space in a column name without it.
# Weaver's own DDL sets the same property, so this is the shape a Weaver table
# is actually installed in rather than a shape invented for the probe.
installed.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).option("delta.columnMapping.mode", "name").option(
    "delta.minReaderVersion", "2"
).option("delta.minWriterVersion", "5").save(path)

business = probe.dataframe()
audited = probe.dataframe(row_audit_columns=True)


def refusal(body):
    """The exception type a call raised, or None when it returned."""

    try:
        body()
    except Exception as raised:
        return type(raised).__name__
    return None


# The authored pattern, `select(*self.columns())`, over a source frame: an
# undeclared column to drop, and a declared one this source never carried,
# completed by an explicit expression.
source = spark.createDataFrame(
    [("o-9", "2026-04-24", "spurious")],
    schema="`Order Id` string, `Order.Date` string, `Extra Undeclared` string",
)
completed = source.select(
    "`Order Id`",
    "`Order.Date`",
    "`Extra Undeclared`",
    F.lit(None).cast("string").alias("Amount"),
).select(*[f"`{name}`" for name in probe.columns()])

emit(
    {
        "columns": list(probe.columns()),
        "primary_key": list(probe.primary_key_columns()),
        "business": business.columns,
        "audited": audited.columns,
        "empty": probe.empty_dataframe().columns,
        "empty_rows": probe.empty_dataframe().count(),
        "dotted_value": business.collect()[0]["Order.Date"],
        "completed": completed.columns,
        "completed_amount": completed.collect()[0]["Amount"],
        # Without that expression, the declared column the source lacks.
        "missing_refused": refusal(
            lambda: source.select(*[f"`{name}`" for name in probe.columns()]).collect()
        ),
        # The pattern as the docstring writes it, unquoted. A dotted name is
        # parsed as a struct field, so this records whether names-only column
        # spellings survive Spark's identifier parsing.
        "unquoted_refused": refusal(
            lambda: source.select(
                *[c for c in probe.columns() if c != "Amount"]
            ).collect()
        ),
    }
)
'''

AUDIT = ["row_insert_datetime", "row_update_datetime", "row_delete_datetime"]
BUSINESS = ["Order Id", "Order.Date", "Amount"]


@weaver_test(hosted=True)
def test_a_table_projects_its_declared_shape_out_of_a_real_delta_table(livy_session):
    seen = livy_session.run(PROBE).payload

    # What the author is told, and what Spark then resolved.
    assert seen["columns"] == BUSINESS
    assert seen["primary_key"] == ["Order Id"]
    assert seen["business"] == BUSINESS
    # The undeclared column is gone and Weaver's own were never offered.
    assert "Extra Undeclared" not in seen["business"]
    assert "row_signature" not in seen["business"]
    assert "row_signature" not in seen["audited"]

    # Opting in adds all three, in their own order, after the business columns.
    assert seen["audited"] == BUSINESS + AUDIT

    # An empty frame is the same shape, and it is empty.
    assert seen["empty"] == BUSINESS
    assert seen["empty_rows"] == 0

    # The dotted name resolved to the column it names rather than to a field of
    # a struct called `Order`, which is what an unquoted identifier would mean.
    assert seen["dotted_value"] == "2026-04-23"

    # The authored pattern: drop what the table does not declare, keep what an
    # explicit expression completed.
    assert seen["completed"] == BUSINESS
    assert seen["completed_amount"] is None

    # And without that expression, the missing column is refused rather than
    # manufactured. Spark's own analysis error, whatever this runtime calls it.
    assert seen["missing_refused"] is not None

    # Why dataframe() quotes what columns() reports: Spark reads the unquoted
    # name as field `Date` of a column `Order` and refuses, even though a column
    # literally named `Order.Date` is there. So `select(*self.columns())`, the
    # authored pattern, is not safe for a dotted column name, which is the case
    # for the declaration parser refusing to accept one.
    assert seen["unquoted_refused"] == "AnalysisException"
