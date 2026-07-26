"""Shared setup for the local Spark suites.

One fixture, and it exists because the catalogue now has to be *addressed*. It
lives in the Weaver Lakehouse, the session is not attached to any Lakehouse in
particular, and locally two Lakehouses that both declare a schema ``_`` are two
different schemas. So a test that reads or writes the catalogue says which
Lakehouse's catalogue it means, exactly as the installer does.
"""

from __future__ import annotations

import pytest

from weaver.spark import SparkCatalogue


@pytest.fixture
def weaver_catalogue(spark, lakehouses):
    """Catalogue operations against this test's own Weaver Lakehouse.

    The schema is dropped afterwards. That is harness isolation, not product
    behaviour: a real installation has one Weaver Lakehouse for the life of the
    session, while this suite presents a succession of temporary directories under
    the same logical name — so without the drop, the second test's tables would
    land in the first test's directory.
    """

    catalogue = SparkCatalogue(
        spark, lakehouses.resolver.spark_destination(lakehouses.weaver)
    )
    catalogue.create_schema("_")
    try:
        yield catalogue
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {catalogue.qualified_schema('_')} CASCADE")
