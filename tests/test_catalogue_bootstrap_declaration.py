"""A catalogue that is not there yet, read as the rows it has.

A Warehouse holding no `_` at all is the ordinary state before the first build,
and it answers as no rows. That is a different state from a missing Warehouse,
and a shared catalogue host goes wrong where the two are conflated: a build
into an existing Warehouse would try to create one, and a missing Warehouse
would look like an empty catalogue and fail far from the cause.

Which of the two a workspace is in is `weaver.initialise`'s question, and
`tests/test_initialise_resources_declaration.py` asks it there.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

from weaver.catalogue.tables import REGISTRY


class _Empty:
    """A catalogue connection to a Warehouse that holds no `_` at all."""

    def shape(self):
        return {}

    def columns_of(self, table):
        return None

    def rows(self, statement):
        raise AssertionError("nothing should be read from a table that is absent")


@weaver_test()
def test_an_absent_table_reads_as_no_rows_rather_than_a_failure():
    """Bootstrap: the build that writes the catalogue is the build that creates it."""

    from weaver.catalogue.reader import read_table

    assert read_table(_Empty(), REGISTRY) == ()


@weaver_test()
def test_an_empty_catalogue_is_not_a_missing_warehouse():
    """The distinction, stated as the two answers a caller gets.

    `prepare_catalogue` says whether the Warehouse had to be made. Whether its
    `_` tables are there is a different question, answered by reading them, and
    a Warehouse that exists with no `_` gives "not created" and "no rows".
    """

    from weaver.catalogue.reader import read_table

    assert _Empty().columns_of(REGISTRY) is None
    assert read_table(_Empty(), REGISTRY) == ()
