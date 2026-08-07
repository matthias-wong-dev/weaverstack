"""Two estates, one process, and the module names they must not share.

The defect this prevents is silent, which is why it is worth this much test.
Authored code says ``from lib.dates import parse_date``; two Lakehouses may each
deploy a ``lib/dates.py``; and Python consults ``sys.modules`` before it searches
any path. So the second estate's load asks for ``lib.dates``, receives the
*first* estate's module, and runs to completion with the wrong helper. No
exception, no warning, and results that are wrong in a way nobody would think to
look for.

What settles it is that the context is part of the key rather than part of the
search — see :mod:`weaver.runtime.python_context`. These prove that, and prove
the ordinary things still work: a module reaches the rest of its own tree, an
import of something outside the tree is untouched, and a failure says which
module and which class.

Pure Python and real files. A deployed tree is a directory, so the whole of this
needs a ``tmp_path`` and nothing else — no session, no Lakehouse, no catalogue.
"""

from __future__ import annotations

import sys
import threading

import pytest

from weaver.errors import LoadError
from weaver.runtime.python_context import (
    ROOT_PACKAGE,
    forget,
    import_deployed_module,
    runtime_context,
)


def tree(root, stamp: str, **extra: str):
    """A deployed runtime tree that says which estate it came from.

    Both halves of the estate carry the stamp — the helper module and the
    ``Files/`` object — because a mix-up in either is the same defect.
    """

    runtime = root / stamp / "Files" / "_" / "Load"
    files = {
        "lib/dates.py": f"STAMP = {stamp!r}\n",
        "Files/Sales__Seed.py": f"class Sales__Seed:\n    stamp = {stamp!r}\n",
        "Sales__Customer.py": (
            "from lib.dates import STAMP\n"
            "from Files.Sales__Seed import Sales__Seed\n"
            "class Sales__Customer:\n"
            "    reached = (STAMP, Sales__Seed.stamp)\n"
        ),
        **extra,
    }
    for relative, text in files.items():
        path = runtime / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return runtime


@pytest.fixture
def raw(tmp_path):
    context = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(tmp_path, "raw"),
    )
    yield context
    forget(context)


@pytest.fixture
def curated(tmp_path):
    context = runtime_context(
        logical_item="Lakehouse/Curated",
        physical_target="Lakehouse/Curated_LH",
        runtime_root=tree(tmp_path, "curated"),
    )
    yield context
    forget(context)


def _customer(context):
    return import_deployed_module(
        context, "Sales__Customer.py", expected="Sales__Customer", node_id="n"
    )


# --- the ordinary case still works --------------------------------------------


def test_a_deployed_module_is_imported_from_its_runtime_root(raw):
    module = _customer(raw)

    assert module.Sales__Customer.__name__ == "Sales__Customer"


def test_a_deployed_module_reaches_the_rest_of_its_own_tree(raw):
    """``Files/`` and ``lib/`` sit where they were authored, so imports read.

    This is why the tree is reproduced verbatim rather than flattened.
    """

    module = _customer(raw)

    assert module.Sales__Customer.reached == ("raw", "raw")


def test_a_deployed_folder_module_is_named_for_its_place_in_the_tree(raw):
    """``Files/Sales__Seed.py`` is ``Files.Sales__Seed``, which is how it is
    imported — naming it otherwise would leave two module objects for one file,
    one of them the object nobody imports."""

    module = import_deployed_module(
        raw, "Files/Sales__Seed.py", expected="Sales__Seed", node_id="n"
    )

    assert module.__name__.endswith(".Files.Sales__Seed")


def test_an_import_of_something_outside_the_tree_is_untouched(tmp_path):
    """Only names the tree defines are redirected.

    Everything else — ``weaver``, ``pyspark``, the standard library — goes to
    the ordinary import, so the redirection cannot make an unrelated import mean
    something new.
    """

    context = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(
            tmp_path,
            "raw",
            **{
                "Sales__Customer.py": (
                    "import json\n"
                    "from weaver.errors import LoadError\n"
                    "class Sales__Customer:\n"
                    "    reached = (json.__name__, LoadError.__name__)\n"
                )
            },
        ),
    )
    try:
        module = _customer(context)

        assert module.Sales__Customer.reached == ("json", "LoadError")
    finally:
        forget(context)


# --- isolation ----------------------------------------------------------------


def test_each_estate_receives_its_own_helper_module(raw, curated):
    """The defect, stated directly."""

    first = _customer(raw)
    second = _customer(curated)

    assert first.Sales__Customer.reached == ("raw", "raw")
    assert second.Sales__Customer.reached == ("curated", "curated")


def test_import_order_does_not_decide_which_estate_wins(raw, curated):
    """Whichever went first, neither is the answer for the other."""

    second = _customer(curated)
    first = _customer(raw)

    assert first.Sales__Customer.reached == ("raw", "raw")
    assert second.Sales__Customer.reached == ("curated", "curated")


def test_two_estates_hold_two_module_objects_for_one_authored_name(raw, curated):
    first = _customer(raw)
    second = _customer(curated)

    assert first is not second
    assert first.__name__ != second.__name__


def test_the_context_is_part_of_the_key_and_not_only_of_the_search(raw, curated):
    """The claim a ``ContextVar`` could not make.

    ``sys.modules`` stays global however the search is scoped, so anything short
    of a distinct *key* leaves the two estates sharing one entry.
    """

    _customer(raw)
    _customer(curated)

    keys = {name for name in sys.modules if name.startswith(f"{ROOT_PACKAGE}.")}

    assert any(name.endswith("lib.dates") for name in keys)
    assert len([name for name in keys if name.endswith(".lib.dates")]) == 2


def test_the_authored_names_never_reach_the_global_module_table(raw, curated):
    """A process that loaded two estates has no bare ``lib.dates`` at all.

    Which is what makes the two importable at once — the alternative, deleting
    ``lib.*`` between loads, needs a global lock and forbids parallel dispatch.
    """

    _customer(raw)
    _customer(curated)

    assert "lib.dates" not in sys.modules
    assert "Files.Sales__Seed" not in sys.modules
    assert "Sales__Customer" not in sys.modules


def test_repeated_loads_keep_reaching_the_same_context(raw, curated):
    first = _customer(raw)
    _customer(curated)
    again = _customer(raw)

    assert again is first
    assert again.Sales__Customer.reached == ("raw", "raw")


def test_objects_deployed_together_share_their_tree(raw):
    """One item in one target is one tree, because that is what its author wrote.

    ``Sales__Customer`` and ``Files.Sales__Seed`` come from the same deployment,
    so the helper each reaches is the same module object.
    """

    customer = _customer(raw)
    seed = import_deployed_module(
        raw, "Files/Sales__Seed.py", expected="Sales__Seed", node_id="n"
    )

    assert sys.modules[raw.qualified("Files.Sales__Seed")] is seed
    assert customer.Sales__Seed is seed.Sales__Seed


def test_concurrent_imports_of_two_estates_do_not_collide(raw, curated):
    """Sequential dispatch is today's shape, not tomorrow's constraint.

    The design must not be what stops the executor running independent nodes at
    once, so the isolation is asserted under threads rather than assumed safe.
    """

    seen: list = []
    failures: list = []

    def load(context, expected):
        try:
            module = _customer(context)
            seen.append((expected, module.Sales__Customer.reached))
        except Exception as exc:  # noqa: BLE001 - reported, not raised, from a thread
            failures.append(exc)

    threads = [
        threading.Thread(target=load, args=(context, stamp))
        for _ in range(8)
        for context, stamp in ((raw, "raw"), (curated, "curated"))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert failures == []
    assert all(reached == (expected, expected) for expected, reached in seen)
    assert len(seen) == 16


# --- what a failure says ------------------------------------------------------


def test_a_module_that_is_not_there_names_the_path_it_was_not_at(raw):
    with pytest.raises(LoadError, match="no deployed module at"):
        import_deployed_module(
            raw, "Sales__Missing.py", expected="Sales__Missing", node_id="n"
        )


def test_a_module_that_will_not_import_is_reported_as_data(tmp_path):
    context = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(
            tmp_path, "raw", **{"Sales__Customer.py": "import no_such_module\n"}
        ),
    )
    try:
        with pytest.raises(LoadError, match="raised ModuleNotFoundError"):
            _customer(context)
    finally:
        forget(context)


def test_a_module_missing_its_declared_class_says_which_one(tmp_path):
    context = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(
            tmp_path, "raw", **{"Sales__Customer.py": "class Wrong:\n    pass\n"}
        ),
    )
    try:
        with pytest.raises(LoadError, match="defines no class 'Sales__Customer'"):
            _customer(context)
    finally:
        forget(context)


def test_a_context_whose_tree_moved_forgets_what_it_held(raw, tmp_path):
    """One item in one target is one tree, and within a session it does not move.

    When it does — a redeployment somewhere else — the modules already imported
    describe the old tree. Rebinding and dropping them is the only answer that
    cannot serve stale code: raising would refuse valid work, and keeping the old
    root would quietly load the wrong estate.
    """

    first = _customer(raw)

    moved = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(tmp_path / "moved", "relocated"),
    )
    try:
        again = _customer(moved)

        assert moved.context_id == raw.context_id
        assert again is not first
        assert again.Sales__Customer.reached == ("relocated", "relocated")
    finally:
        forget(moved)


# --- the context name ---------------------------------------------------------


def test_a_context_names_the_estate_it_came_from(raw):
    """Deterministic and legible, because it appears in every traceback."""

    assert raw.context_id == "lakehouse_raw__lakehouse_raw_lh"
    assert raw.package == f"{ROOT_PACKAGE}.lakehouse_raw__lakehouse_raw_lh"


def test_the_same_item_and_target_always_produce_the_same_context():
    first = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root="/anywhere",
    )
    second = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root="/anywhere",
    )

    assert first.context_id == second.context_id


def test_two_targets_of_one_item_are_two_contexts():
    """The same repository built into two Lakehouses is two estates."""

    first = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root="/a",
    )
    second = runtime_context(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Other_LH",
        runtime_root="/b",
    )

    assert first.context_id != second.context_id
