"""The two generic entry points a person calls in a Warehouse.

Every object gets a procedure of its own, and those record nothing: an
orchestrated run records what settled centrally. ``_.Load`` and ``_.Test`` are
what runs one by hand and leaves the same record.

.. code-block:: text

    exec _.[Load] @object_name = 'Sales.Customer'
    exec _.[Test] @object_name = 'Sales.OrdersReconcile'

Pure Python: these are generated from the item's declarations, so what they
contain is a decision every input to which can be constructed. That a Fabric
Warehouse accepts them is a claim about Fabric and is made where there is one.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.tables import (
    BOOKMARK,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    TEST_STATUS,
)
from weaver.declaration.metadata import ASSUMPTION, TEST, ObjectId
from weaver.declaration.model import WeaverItemId
from weaver.declaration.tsql_entry import (
    WEAVER_ERROR_MAX,
    WEAVER_ERROR_MIN,
    generate_load_entry,
    generate_test_entry,
)
from weaver.errors import DiscoveryError

ITEM = WeaverItemId("Warehouse", "Reporting")
CUSTOMER = ObjectId("Sales", "Customer")
ORDER = ObjectId("Sales", "Order")
RECONCILE = ObjectId("Sales", "OrdersReconcile")
UP_TO_DATE = ObjectId("Sales", "OrdersUpToDate")


@pytest.fixture
def load_script() -> str:
    return generate_load_entry(ITEM, [CUSTOMER, ORDER])


@pytest.fixture
def validation_script() -> str:
    return generate_test_entry(ITEM, {RECONCILE: TEST, UP_TO_DATE: ASSUMPTION})


# --- what they are -------------------------------------------------------------


@weaver_test()
def test_the_entry_points_are_named_for_what_they_do(load_script, validation_script):
    """``exec _.[Load]`` and ``exec _.[Test]``, and nothing else generic."""

    assert "create or alter procedure [_].[Load]" in load_script
    assert "create or alter procedure [_].[Test]" in validation_script


@weaver_test()
def test_there_is_no_generic_assumption_entry_point(validation_script):
    """``_.Test`` is the one validation entry point, for both kinds.

    A person asking about a validation by name should not have to know which kind
    it was declared as.
    """

    assert "create or alter procedure [_].[Assumption]" not in validation_script
    assert "[_].[Test Sales.OrdersReconcile]" in validation_script
    assert "[_].[Assumption Sales.OrdersUpToDate]" in validation_script


@weaver_test()
def test_which_kind_a_validation_is_comes_from_the_declaration(validation_script):
    """Settled at generation, never probed from the estate at run time.

    Written as the ``_`` schema stores it. Python writes the internal value and
    the catalogue's renderer maps it at the persistence boundary; generated SQL
    crosses no such boundary, so it writes what the column holds and the two
    agree about a row a reader may have got from either.
    """

    assert "set @weaver_test_type = N'Test';" in validation_script
    assert "set @weaver_test_type = N'Assumption';" in validation_script
    assert "object_id(" not in validation_script


@weaver_test()
def test_a_name_that_matched_nothing_records_nothing(load_script, validation_script):
    """The refusal is about the request rather than about a load.

    Nothing ran, so there is no outcome and no object to record one against —
    and every identity column is not null, so a row would be refused by the
    catalogue anyway and would hide the message saying what went wrong. So it is
    remembered inside the TRY and raised after it, ahead of every write.
    """

    for script in (load_script, validation_script):
        assert "set @weaver_unmatched = concat(@object_name" in script
        raised = script.index("throw 51030, @weaver_unmatched, 1;")
        assert raised < script.index("merge into")


@weaver_test()
def test_dispatch_is_a_static_chain_and_not_dynamic_sql(load_script, validation_script):
    """Generated for one item, so the objects it installs are known.

    Which is what lets the lower procedure's output parameters be read directly,
    and makes a name the item does not install a refusal rather than a failure
    inside a string.
    """

    for script in (load_script, validation_script):
        assert "sp_executesql" not in script
        assert "exec (" not in script
    assert "if @object_name = N'Sales.Customer'" in load_script
    assert "else if @object_name = N'Sales.Order'" in load_script


@weaver_test()
def test_an_object_the_item_does_not_install_is_refused(load_script, validation_script):
    """Reporting a row for it would put an object in the estate's own record
    that the estate has never had."""

    assert "is not a loadable object in this Warehouse" in load_script
    assert "is not a validation in this Warehouse" in validation_script
    assert "throw 51030" in load_script


@weaver_test()
def test_an_item_with_nothing_to_wrap_gets_no_entry_point():
    with pytest.raises(DiscoveryError, match="installs no loads"):
        generate_load_entry(ITEM, [])
    with pytest.raises(DiscoveryError, match="installs no validations"):
        generate_test_entry(ITEM, {})


# --- the outcome they record ---------------------------------------------------


@weaver_test()
def test_the_lower_procedure_runs_inside_try_catch(load_script, validation_script):
    """A refusal is an outcome to record, not a reason to leave no record."""

    for script in (load_script, validation_script):
        assert "begin try" in script
        assert "set @weaver_error = error_message();" in script
        assert "set @weaver_error_number = error_number();" in script


@weaver_test()
def test_a_refusal_weaver_threw_is_failed_and_anything_else_is_an_error(load_script):
    """The same line the Python side draws with ``isinstance(exc, WeaverError)``.

    A stability threshold breached or rows refused ran under Weaver's control and
    produced an unacceptable result; anything else could not be evaluated.
    """

    assert (
        f"case when @weaver_error_number between {WEAVER_ERROR_MIN} and "
        f"{WEAVER_ERROR_MAX} then N'Failed'" in load_script
    )
    assert "when @weaver_error is not null then N'Error'" in load_script


@weaver_test()
def test_a_load_reports_every_outcome_the_vocabulary_has_for_one(load_script):
    assert "when @is_static_skip = 1 then N'Skipped'" in load_script
    assert "when @succeeded = 1 then N'Succeeded'" in load_script
    assert "when @rows_rejected > 0 then N'Rejected'" in load_script


@weaver_test()
def test_a_validation_that_could_not_be_evaluated_reports_no_failure_count(
    validation_script,
):
    """Zero discrepancies is the one answer a validation must never invent."""

    assert "when @weaver_error is not null then null" in validation_script
    assert "when @weaver_failure_count > 0 then N'Failed'" in validation_script


@weaver_test()
def test_a_load_writes_its_whole_operational_record(load_script):
    for table in (LOG, LOAD_STATUS, LOAD_STATISTIC, BOOKMARK):
        assert f"merge into [_].[{table.name}]" in load_script, table.name
    assert f"merge into [_].[{TEST_STATUS.name}]" not in load_script


@weaver_test()
def test_a_validation_writes_its_evidence_and_its_status_and_no_load_state(
    validation_script,
):
    for table in (LOG, TEST_STATUS):
        assert f"merge into [_].[{table.name}]" in validation_script, table.name
    for table in (LOAD_STATUS, LOAD_STATISTIC, BOOKMARK):
        assert f"merge into [_].[{table.name}]" not in validation_script, table.name


@weaver_test()
def test_the_bookmark_advances_only_for_a_clean_load_that_read_a_window(load_script):
    """A rejecting load keeps the bookmark it had; a Static skip moves nothing."""

    assert (
        "if @weaver_result = N'Succeeded' and @bookmark_datetime is not null"
        in load_script
    )


@weaver_test()
def test_every_write_is_a_merge_because_the_tables_may_be_views(load_script):
    """Fabric refuses a plain INSERT through a cross-database view.

    In every Warehouse but the catalogue's own these tables are exactly that, so
    an appended row merges on a surrogate generated a moment ago and is never
    matched.
    """

    assert "insert into" not in load_script.casefold()
    assert load_script.count("merge into") == 4
    assert "using (select\n        @weaver_log_sk as [Log SK]" in load_script


@weaver_test()
def test_the_recorded_identity_is_the_items_own_and_the_dispatched_object(load_script):
    """Baked in for the item, taken from the branch for the object."""

    assert "N'Warehouse' as [Item type]" in load_script
    assert "N'Reporting' as [Item name]" in load_script
    assert "@weaver_schema as [Schema name]" in load_script
    assert "set @weaver_schema = N'Sales';" in load_script
    assert "set @weaver_object = N'Customer';" in load_script


@weaver_test()
def test_a_standalone_call_is_its_own_workflow(load_script):
    """A workflow is its rows, and a call by hand is a workflow of one."""

    assert (
        "declare @weaver_workflow varchar(128) = cast(newid() as varchar(36));"
        in load_script
    )


@weaver_test()
def test_the_statistic_says_a_reload_is_not_what_this_was(load_script):
    """Written rather than left null, so a reader counting reloads gets zero."""

    assert "cast(0 as bit)" in load_script
    assert "cast(coalesce(@is_static_skip, 0) as bit)" in load_script


@weaver_test()
def test_the_load_script_takes_the_policy_a_caller_may_choose(load_script):
    """Fault tolerance and the stability waiver, passed straight through."""

    assert "@fault_tolerant bit = 0" in load_script
    assert "@ignore_stability_threshold bit = 0" in load_script
    assert "@fault_tolerant = @fault_tolerant" in load_script


@weaver_test()
def test_no_column_may_be_left_unsupplied():
    """A table gaining a column has to be given a value here as well.

    Otherwise the generated MERGE would insert a row missing the column, which
    reads as a row that had nothing to say about it.
    """

    from weaver.declaration.tsql_entry import _write

    with pytest.raises(DiscoveryError, match="no value was supplied"):
        _write(LOAD_STATUS, {"item_type": "N'Warehouse'"}, keyed=True)


__all__: tuple = ()
