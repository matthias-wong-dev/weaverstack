"""The load contract primitive — what one object's load is told, and by whom.

Two claims, and the second is the interesting one.

The contract must be *derived*, so that a Warehouse procedure and a Python table
built from the same header cannot come to disagree about what it meant. And it
must be readable by a module that has nothing else: an installed
``Sales__Customer.py`` in a session has no repository to reopen, no catalogue to
query and no bundle to consult, so if its docstring is not sufficient the load
primitives are not primitives at all.

Nothing here needs Spark. A contract is plain data read out of text.
"""

from __future__ import annotations

import textwrap
import types

import pytest
from support.weaver_test import weaver_test

from weaver.declaration.metadata import (
    PYTHON,
    SPARK_SQL,
    SQL,
    parse_document,
)
from weaver.errors import LoadError, MetadataError
from weaver.runtime import FolderLoadContract, LoadContract, document_for_module
from weaver.runtime.load_contract import normalise_read_result

TABLE_HEADER = """
Table ID: Sales.Customer

Description: One row per customer.

Lineage: The sales system.

Primary key: Customer id

Schema:
  Customer id: string
  Customer name: string
  Amount: decimal(18,2)
"""


def _document(header: str, *, language: str = PYTHON):
    return parse_document(textwrap.dedent(header).lstrip(), language=language)


def _module(docstring: str, name: str = "Sales__Customer"):
    """An installed module, as a load actually meets one: an object with a docstring.

    Constructed rather than imported because the subject is what ``load()`` reads
    off a module, not how Python found the file.
    """

    module = types.ModuleType(name)
    module.__doc__ = docstring
    return module


# --- derived from the declaration -------------------------------------------


@weaver_test()
def test_the_contract_carries_what_a_load_needs_and_no_more():
    contract = LoadContract.from_document(_document(TABLE_HEADER))

    assert contract.qualified == "Sales.Customer"
    assert contract.primary_key == ("Customer id",)
    # Undeclared comparison columns default to every non-key business column.
    assert contract.comparison_columns == ("Customer name", "Amount")
    assert contract.incremental is False
    assert contract.identity_column is None


@weaver_test()
def test_declared_comparison_columns_narrow_the_change_test():
    contract = LoadContract.from_document(
        _document(TABLE_HEADER + "\nComparison columns: Amount\n")
    )

    assert contract.comparison_columns == ("Amount",)


@weaver_test()
def test_a_warehouse_table_carries_its_identity_column():
    """Only a Warehouse table has one, so only this contract names it.

    It matters to the load because it is the one column an insert must *not*
    name: the engine generates it. It sits beside the primary key rather than
    being it — a load could never match on a column the engine assigns.
    """

    header = TABLE_HEADER.replace(
        "Primary key: Customer id", "Primary key: Customer id\n\nIdentity: Customer key"
    )
    contract = LoadContract.from_document(_document(header, language=SQL))

    assert contract.identity_column == "Customer key"


# --- what follows from the key ----------------------------------------------


@weaver_test()
def test_no_primary_key_means_full_replacement():
    header = TABLE_HEADER.replace("Primary key: Customer id\n", "")
    contract = LoadContract.from_document(_document(header))

    assert contract.replaces_wholesale is True
    assert contract.deletes_absent_rows is False


@weaver_test()
def test_a_keyed_load_deletes_rows_the_source_stopped_producing():
    contract = LoadContract.from_document(_document(TABLE_HEADER))

    assert contract.replaces_wholesale is False
    assert contract.deletes_absent_rows is True


@weaver_test()
def test_an_incremental_load_never_deletes():
    """Absence from an incremental source says nothing about existence.

    An incremental source is a window on the truth, not the whole of it, so a
    row missing from it has not been retired — it was simply not in the window.
    """

    contract = LoadContract.from_document(
        _document(TABLE_HEADER + "\nIncremental: true\n")
    )

    assert contract.incremental is True
    assert contract.deletes_absent_rows is False


# --- uniqueness and nullability ---------------------------------------------


@weaver_test()
def test_unique_keys_are_optional():
    """Most tables declare none, and the load must generate no machinery for them."""

    contract = LoadContract.from_document(_document(TABLE_HEADER))

    assert contract.unique_keys == ()
    assert contract.checks_merge_uniqueness is False


@weaver_test()
def test_unique_keys_reach_the_runtime_in_declaration_order():
    """Order is contract, not presentation.

    A row rejected by an earlier key does not go on to choose the survivor of a
    later one, so which key came first decides which rows survive.
    """

    contract = LoadContract.from_document(
        _document(
            TABLE_HEADER
            + "\nUnique keys:\n  - Customer name\n  - Amount, Customer name\n"
        )
    )

    assert contract.unique_keys == (
        ("Customer name",),
        ("Amount", "Customer name"),
    )


@weaver_test()
def test_a_composite_unique_key_keeps_its_column_order():
    contract = LoadContract.from_document(
        _document(TABLE_HEADER + "\nUnique keys:\n  - Amount, Customer name\n")
    )

    assert contract.unique_keys == (("Amount", "Customer name"),)


@weaver_test()
def test_only_declared_not_null_columns_reach_the_runtime():
    """A business column is nullable unless the declaration says otherwise.

    The primary key is stronger than not-null and is validated separately, so it
    is not repeated here — counting it twice would let one blank key inflate the
    rejection threshold.
    """

    contract = LoadContract.from_document(
        _document(TABLE_HEADER + "\nNot null:\n  - Customer name\n")
    )

    assert contract.not_null_columns == ("Customer name",)
    assert "Amount" not in contract.not_null_columns
    assert "Customer id" not in contract.not_null_columns


@weaver_test()
def test_a_table_declaring_nothing_not_null_has_no_null_columns_to_check():
    contract = LoadContract.from_document(_document(TABLE_HEADER))

    assert contract.not_null_columns == ()


@weaver_test()
def test_merge_uniqueness_is_checked_only_when_incremental_and_unique():
    """The one place a load can refuse outright rather than reject rows.

    A non-incremental load leaves the target equal to clean staging, which has
    already been made unique, so it has nothing to ask.
    """

    unique = "\nUnique keys:\n  - Customer name\n"
    incremental = "\nIncremental: true\n"

    def contract(extra: str):
        return LoadContract.from_document(_document(TABLE_HEADER + extra))

    assert contract(unique + incremental).checks_merge_uniqueness is True
    assert contract(unique).checks_merge_uniqueness is False
    assert contract(incremental).checks_merge_uniqueness is False


@weaver_test()
def test_an_unkeyed_table_carries_no_uniqueness_machinery():
    """Full replacement is unchanged by this work."""

    header = TABLE_HEADER.replace("Primary key: Customer id\n", "")
    contract = LoadContract.from_document(
        _document(header + "\nUnique keys:\n  - Customer name\n")
    )

    assert contract.replaces_wholesale is True
    assert contract.unique_keys == (("Customer name",),)
    assert contract.checks_merge_uniqueness is False


@weaver_test()
def test_the_reject_vocabulary_names_the_column_or_key_that_refused_a_row():
    """A table may declare several unique keys, so "duplicate" alone is not enough."""

    from weaver.runtime.load_contract import (
        duplicate_unique_reason,
        null_column_reason,
    )

    assert null_column_reason("Customer name") == "null_column: Customer name"
    assert (
        duplicate_unique_reason(("Region id", "External ref"))
        == "duplicate_unique_key: Region id, External ref"
    )


@weaver_test()
def test_a_folder_contract_carries_its_file_key_and_policy():
    header = """
    Folder ID: Raw.CustomerCsv

    Description: Customer extracts.

    Lineage: The sales system.

    File key: "*.csv"

    Incremental: false
    """
    contract = FolderLoadContract.from_document(_document(header))

    assert contract.qualified == "Raw.CustomerCsv"
    assert contract.file_keys == ("*.csv",)
    assert contract.replaces_wholesale is True


@weaver_test()
def test_a_folder_is_not_a_table_and_says_so():
    folder = _document(
        """
        Folder ID: Raw.CustomerCsv

        Description: x

        Lineage: y

        File key: "*.csv"
        """
    )
    with pytest.raises(LoadError, match="has no table load contract"):
        LoadContract.from_document(folder)


# --- read result -------------------------------------------------------------


@weaver_test()
def test_a_single_read_value_means_no_explicit_deletes():
    staged = object()

    assert normalise_read_result(staged) == (staged, None)


@weaver_test()
def test_an_explicit_read_pair_is_preserved():
    staged, deletes = object(), object()

    assert normalise_read_result((staged, deletes)) == (staged, deletes)


@pytest.mark.parametrize("returned", [(), (object(),), (object(), object(), object())])
@weaver_test()
def test_a_malformed_explicit_read_tuple_is_refused(returned):
    with pytest.raises(LoadError, match="return data, or \\(data, deletes\\)"):
        normalise_read_result(returned)


# --- read from an installed module ------------------------------------------


@weaver_test()
def test_an_installed_module_carries_its_own_contract():
    """The whole point: a deployed module is sufficient by itself.

    No repository is opened and no catalogue is read — the docstring the author
    wrote is what the load runs from, which is what makes `.load()` a primitive
    rather than the tail end of an orchestration.
    """

    module = _module(textwrap.dedent(TABLE_HEADER).lstrip())
    contract = LoadContract.from_document(document_for_module(module))

    assert contract.qualified == "Sales.Customer"
    assert contract.primary_key == ("Customer id",)


@weaver_test()
def test_an_indented_docstring_reads_the_same_as_the_repository_reads_it():
    """A module docstring is indented in the file; the contract must not care.

    The repository reader dedents with `ast.get_docstring(clean=True)`, so the
    runtime has to reproduce that or the same file would mean two things.
    """

    module = _module(
        "\n    Table ID: Sales.Customer\n\n"
        "    Description: One row per customer.\n\n"
        "    Lineage: The sales system.\n\n"
        "    Primary key: Customer id\n\n"
        "    Schema:\n      Customer id: string\n      Amount: decimal(18,2)\n    "
    )
    contract = LoadContract.from_document(document_for_module(module))

    assert contract.primary_key == ("Customer id",)
    assert contract.comparison_columns == ("Amount",)


@weaver_test()
def test_a_module_edited_after_deployment_is_read_as_it_now_stands():
    """Metadata changes are visible immediately, with no rebuild in between.

    This is what makes a notebook loop workable: edit the docstring, reload, run
    `.load()`. It is also why the module is at the operator's risk — nothing
    revalidates it against the repository it came from.
    """

    module = _module(textwrap.dedent(TABLE_HEADER).lstrip())
    assert LoadContract.from_document(document_for_module(module)).incremental is False

    module.__doc__ = textwrap.dedent(TABLE_HEADER + "\nIncremental: true\n").lstrip()

    assert LoadContract.from_document(document_for_module(module)).incremental is True


@weaver_test()
def test_a_module_with_no_metadata_block_is_refused_by_name():
    with pytest.raises(LoadError, match="Sales__Customer carries no Weaver metadata"):
        document_for_module(_module(""))


@weaver_test()
def test_the_runtime_parser_still_refuses_a_broken_contract():
    """Runtime parsing is narrower than the repository's, not laxer.

    It does not check filenames, classes or dependencies — those were settled
    before installation. What a load depends on it still validates, so a header
    that cannot describe a load is refused here rather than misread.
    """

    module = _module("Table ID: Sales.Customer\n\nIncremental: true\n")
    with pytest.raises(MetadataError):
        document_for_module(module)


@pytest.mark.parametrize("language", [PYTHON, SPARK_SQL])
@weaver_test()
def test_a_delta_table_contract_never_names_an_identity_column(language):
    header = TABLE_HEADER + ("\nDependencies: []\n" if language == SPARK_SQL else "")
    contract = LoadContract.from_document(_document(header, language=language))

    assert contract.identity_column is None
