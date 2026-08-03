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

from weaver.declaration.metadata import (
    PYTHON,
    SPARK_SQL,
    SQL,
    parse_document,
)
from weaver.errors import LoadError, MetadataError
from weaver.runtime import FolderLoadContract, LoadContract, document_for_module

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


def test_the_contract_carries_what_a_load_needs_and_no_more():
    contract = LoadContract.from_document(_document(TABLE_HEADER))

    assert contract.qualified == "Sales.Customer"
    assert contract.primary_key == ("Customer id",)
    # Undeclared comparison columns default to every non-key business column.
    assert contract.comparison_columns == ("Customer name", "Amount")
    assert contract.incremental is False
    assert contract.identity_column is None


def test_declared_comparison_columns_narrow_the_change_test():
    contract = LoadContract.from_document(
        _document(TABLE_HEADER + "\nComparison columns: Amount\n")
    )

    assert contract.comparison_columns == ("Amount",)


def test_a_warehouse_table_carries_its_identity_column():
    """Only a Warehouse table has one, so only this contract names it.

    It matters to the load because it is the one column an insert must *not*
    name: the engine generates it.
    """

    header = TABLE_HEADER.replace(
        "Primary key: Customer id", "Primary key: Customer key\n\nIdentity: Customer key"
    )
    contract = LoadContract.from_document(_document(header, language=SQL))

    assert contract.identity_column == "Customer key"


# --- what follows from the key ----------------------------------------------


def test_no_primary_key_means_full_replacement():
    header = TABLE_HEADER.replace("Primary key: Customer id\n", "")
    contract = LoadContract.from_document(_document(header))

    assert contract.replaces_wholesale is True
    assert contract.deletes_absent_rows is False


def test_a_keyed_load_deletes_rows_the_source_stopped_producing():
    contract = LoadContract.from_document(_document(TABLE_HEADER))

    assert contract.replaces_wholesale is False
    assert contract.deletes_absent_rows is True


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


# --- read from an installed module ------------------------------------------


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


def test_a_module_with_no_metadata_block_is_refused_by_name():
    with pytest.raises(LoadError, match="Sales__Customer carries no Weaver metadata"):
        document_for_module(_module(""))


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
def test_a_delta_table_contract_never_names_an_identity_column(language):
    header = TABLE_HEADER + ("\nDependencies: []\n" if language == SPARK_SQL else "")
    contract = LoadContract.from_document(_document(header, language=language))

    assert contract.identity_column is None
