"""What the fixed ``_.Load`` and ``_.Test`` contain.

Pure Python: the entry points are generated from nothing but Weaver's own
contract, so every claim here is made against the bytes a Warehouse installs.
That a Fabric Warehouse accepts them is a claim about Fabric and is made where
there is one.

The property underneath all of them: the entry points are fixed. Nothing about
the objects an item installs is enumerated into their text, so adding or
removing a load or a validation cannot change what they are.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.catalogue.tables import (
    BOOKMARK,
    INSTALLATION,
    LOAD_STATISTIC,
    LOAD_STATUS,
    LOG,
    TEST_STATUS,
)
from weaver.declaration.tsql_entry import (
    RETHROW_ERROR,
    WEAVER_ERROR_MAX,
    WEAVER_ERROR_MIN,
    generate_load_entry,
    generate_test_entry,
)

#: The branch that calls Weaver's own refusal Failed. A load has it; a validation
#: does not.
_WEAVER_RANGE = (
    f"case when @weaver_error_number between {WEAVER_ERROR_MIN} and "
    f"{WEAVER_ERROR_MAX} then N'Failed'"
)


@pytest.fixture
def load_script() -> str:
    return generate_load_entry()


@pytest.fixture
def validation_script() -> str:
    return generate_test_entry()


# --- what they are -------------------------------------------------------------


@weaver_test()
def test_the_entry_points_are_named_for_what_they_do(load_script, validation_script):
    """``exec _.[Load]`` and ``exec _.[Test]``, and nothing else generic."""

    assert "create or alter procedure [_].[Load]" in load_script
    assert "create or alter procedure [_].[Test]" in validation_script


@weaver_test()
def test_there_is_no_generic_assumption_entry_point(validation_script):
    """``_.Test`` is the one validation entry point, for both kinds."""

    assert "create or alter procedure [_].[Assumption]" not in validation_script


@weaver_test()
def test_dispatch_reads_the_estate_instead_of_enumerating_it(
    load_script, validation_script
):
    """object_id answers whether the implementation procedure exists."""

    for script in (load_script, validation_script):
        assert "object_id(@weaver_target" in script or (
            "object_id(@weaver_test_procedure" in script
        )
        assert "sp_executesql" in script
    assert "N'Load ' + @object_name" in load_script
    # _.Test asks both kinds and lets exactly one exist.
    assert "N'Test ' + @object_name" in validation_script
    assert "N'Assumption ' + @object_name" in validation_script


@weaver_test()
def test_a_name_that_matched_nothing_records_nothing(load_script, validation_script):
    """Nothing ran, so there is no object to record an outcome against.

    Remembered inside the TRY and raised after it, ahead of every write.
    """

    for script in (load_script, validation_script):
        raised = script.index("throw 51030, @weaver_unmatched, 1;")
        assert raised < script.index("merge into")


@weaver_test()
def test_an_unknown_object_is_refused(load_script, validation_script):
    """A name this Warehouse does not hold cannot be executed."""

    assert "is not a loadable object in this Warehouse" in load_script
    assert "is not a validation in this Warehouse" in validation_script


@weaver_test()
def test_both_kinds_at_once_is_refused_as_broken(validation_script):
    """Exactly one of _.[Test X.Y] and _.[Assumption X.Y] may exist."""

    assert "is installed as both a Test and an Assumption" in validation_script


@weaver_test()
def test_the_kind_comes_from_which_procedure_exists(validation_script):
    """Written as the ``_`` schema stores it, from physical existence alone."""

    assert "set @weaver_test_type = N'Test';" in validation_script
    assert "set @weaver_test_type = N'Assumption';" in validation_script


@weaver_test()
def test_a_malformed_object_name_is_refused(load_script):
    """One dot separates a schema from its object; there is no second reading."""

    assert "is not a Schema.Object name" in load_script
    assert "charindex('.', @object_name)" in load_script


# --- identity ------------------------------------------------------------------


@weaver_test()
def test_identity_comes_from_the_parameter_or_from_installation(
    load_script, validation_script
):
    """Runner mode supplies it; a call by hand resolves it through the estate."""

    for script in (load_script, validation_script):
        assert "@item_name varchar(128) = null" in script
        assert f"from [_].[{INSTALLATION.name}]" in script
        assert "[Item type] = N'Warehouse'" in script
        assert "[Target name] = db_name()" in script
        # An ambiguous estate refuses rather than guesses.
        assert "is not unique in _.Installation; supply @item_name" in script


@weaver_test()
def test_the_recorded_item_is_never_baked_in(load_script):
    """Every write names @item_name, so both caller modes record correctly."""

    assert "@item_name as [Item name]" in load_script
    assert "N'Reporting'" not in load_script


@weaver_test()
def test_no_generated_implementation_abi_change_for_identity(load_script):
    """The implementation procedure keeps its own signature; nothing is added."""

    # The dynamic call carries only the policy inputs and the result outputs.
    assert "@fault_tolerant = @fault_tolerant" in load_script
    assert "@ignore_stability_threshold = @ignore_stability_threshold" in load_script
    for logical in ("succeeded", "rows_read", "bookmark_datetime", "is_static_skip"):
        assert f"@weaver_{logical} = @{logical} output" in load_script
    call = load_script[
        load_script.index("set @weaver_call"): load_script.index("end try")
    ]
    assert "item_name" not in call


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
    """The same line the Python side draws with ``isinstance(exc, WeaverError)``."""

    assert _WEAVER_RANGE in load_script
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
def test_a_validation_that_threw_is_an_error_whatever_threw_it(
    load_script, validation_script
):
    """Where the two kinds of work part, and both halves are asserted.

    A load can refuse and mean it. A validation that threw produced no judgement
    at all, a shape mismatch, a key that repeats, so there is nothing for
    Failed to mean, and Weaver's own refusal range has no branch here.
    """

    assert _WEAVER_RANGE in load_script
    assert _WEAVER_RANGE not in validation_script
    assert "case when @weaver_error is not null then N'Error'" in validation_script


@weaver_test()
def test_what_the_lower_procedure_raised_is_raised_again_after_the_record(
    load_script, validation_script
):
    """Returning normally would make a failure look like a successful call."""

    for script in (load_script, validation_script):
        raising = script.index("throw @weaver_number, @weaver_rethrow, 1;")
        assert raising > script.rindex("merge into")
        # Only what was thrown. A returned outcome is an answer, not a failure.
        assert "if @weaver_error is not null" in script
        assert "@rows_rejected" not in script[raising:]
        # The original number where THROW accepts it, so a caller can still match
        # on Weaver's own refusal codes.
        assert "case when @weaver_error_number >= 50000" in script
        assert str(RETHROW_ERROR) in script


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
    """Fabric refuses a plain INSERT through a cross-database view."""

    assert "insert into" not in load_script.casefold()
    assert load_script.count("merge into") == 4
    assert "using (select\n        @weaver_log_sk as [Log SK]" in load_script


@weaver_test()
def test_the_recorded_identity_is_the_dispatched_object(load_script):
    """The object half comes from the request; the item half from resolution."""

    assert "@weaver_schema as [Schema name]" in load_script
    assert "@weaver_object as [Object name]" in load_script


@weaver_test()
def test_a_standalone_call_is_its_own_workflow(load_script):
    """A call by hand is a workflow of one."""

    assert (
        "declare @weaver_workflow varchar(128) = cast(newid() as varchar(36));"
        in load_script
    )


@weaver_test()
def test_the_statistic_says_a_reload_is_not_what_this_was(load_script):
    """Written rather than left null, so counting reloads gives zero."""

    assert "cast(0 as bit)" in load_script
    assert "cast(coalesce(@is_static_skip, 0) as bit)" in load_script


@weaver_test()
def test_the_load_entry_takes_the_policy_a_caller_may_choose(load_script):
    """Fault tolerance and the stability waiver, passed straight through."""

    assert "@fault_tolerant bit = 0" in load_script
    assert "@ignore_stability_threshold bit = 0" in load_script


@weaver_test()
def test_no_column_may_be_left_unsupplied():
    """A table gaining a column has to be given a value here as well."""

    from weaver.declaration.tsql_entry import _write

    with pytest.raises(Exception, match="no value was supplied"):
        _write(LOAD_STATUS, {"item_type": "N'Warehouse'"}, keyed=True)


__all__: tuple = ()
