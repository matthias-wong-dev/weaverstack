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
from support.weaver_test import weaver_test

from weaver.errors import LoadError
from weaver.runtime.python_context import (
    ROOT_PACKAGE,
    RuntimeScope,
    import_deployed_module,
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
def scope():
    """One run's worth of contexts, closed when the run would end."""

    with RuntimeScope.new() as opened:
        yield opened


@pytest.fixture
def raw(scope, tmp_path):
    return scope.context_for(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(tmp_path, "raw"),
    )


@pytest.fixture
def curated(scope, tmp_path):
    return scope.context_for(
        logical_item="Lakehouse/Curated",
        physical_target="Lakehouse/Curated_LH",
        runtime_root=tree(tmp_path, "curated"),
    )


def _customer(context):
    return import_deployed_module(
        context, "Sales__Customer.py", expected="Sales__Customer", node_id="n"
    )


# --- the ordinary case still works --------------------------------------------


@weaver_test()
def test_a_deployed_module_is_imported_from_its_runtime_root(raw):
    module = _customer(raw)

    assert module.Sales__Customer.__name__ == "Sales__Customer"


@weaver_test()
def test_a_deployed_module_reaches_the_rest_of_its_own_tree(raw):
    """``Files/`` and ``lib/`` sit where they were authored, so imports read.

    This is why the tree is reproduced verbatim rather than flattened.
    """

    module = _customer(raw)

    assert module.Sales__Customer.reached == ("raw", "raw")


@weaver_test()
def test_a_deployed_folder_module_is_named_for_its_place_in_the_tree(raw):
    """``Files/Sales__Seed.py`` is ``Files.Sales__Seed``, which is how it is
    imported — naming it otherwise would leave two module objects for one file,
    one of them the object nobody imports."""

    module = import_deployed_module(
        raw, "Files/Sales__Seed.py", expected="Sales__Seed", node_id="n"
    )

    assert module.__name__.endswith(".Files.Sales__Seed")


@weaver_test()
def test_an_import_of_something_outside_the_tree_is_untouched(scope, tmp_path):
    """Only names the tree defines are redirected.

    Everything else — ``weaver``, ``pyspark``, the standard library — goes to
    the ordinary import, so the redirection cannot make an unrelated import mean
    something new.
    """

    context = scope.context_for(
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

    module = _customer(context)

    assert module.Sales__Customer.reached == ("json", "LoadError")


# --- isolation ----------------------------------------------------------------


@weaver_test()
def test_each_estate_receives_its_own_helper_module(raw, curated):
    """The defect, stated directly."""

    first = _customer(raw)
    second = _customer(curated)

    assert first.Sales__Customer.reached == ("raw", "raw")
    assert second.Sales__Customer.reached == ("curated", "curated")


@weaver_test()
def test_import_order_does_not_decide_which_estate_wins(raw, curated):
    """Whichever went first, neither is the answer for the other."""

    second = _customer(curated)
    first = _customer(raw)

    assert first.Sales__Customer.reached == ("raw", "raw")
    assert second.Sales__Customer.reached == ("curated", "curated")


@weaver_test()
def test_two_estates_hold_two_module_objects_for_one_authored_name(raw, curated):
    first = _customer(raw)
    second = _customer(curated)

    assert first is not second
    assert first.__name__ != second.__name__


@weaver_test()
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


@weaver_test()
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


@weaver_test()
def test_repeated_loads_keep_reaching_the_same_context(raw, curated):
    first = _customer(raw)
    _customer(curated)
    again = _customer(raw)

    assert again is first
    assert again.Sales__Customer.reached == ("raw", "raw")


@weaver_test()
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


@weaver_test()
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


@weaver_test()
def test_a_module_that_is_not_there_names_the_path_it_was_not_at(raw):
    with pytest.raises(LoadError, match="no deployed module at"):
        import_deployed_module(
            raw, "Sales__Missing.py", expected="Sales__Missing", node_id="n"
        )


@weaver_test()
def test_a_module_that_will_not_import_is_reported_as_data(scope, tmp_path):
    context = scope.context_for(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(
            tmp_path, "raw", **{"Sales__Customer.py": "import no_such_module\n"}
        ),
    )

    with pytest.raises(LoadError, match="raised ModuleNotFoundError"):
        _customer(context)


@weaver_test()
def test_a_module_missing_its_declared_class_says_which_one(scope, tmp_path):
    context = scope.context_for(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(
            tmp_path, "raw", **{"Sales__Customer.py": "class Wrong:\n    pass\n"}
        ),
    )

    with pytest.raises(LoadError, match="defines no class 'Sales__Customer'"):
        _customer(context)


# --- what a run does not carry into the next one ------------------------------


@weaver_test()
def test_a_rebuilt_module_is_executed_by_the_next_run(tmp_path):
    """The regression this lifetime exists for.

    A Fabric session outlives a build, and a build rewrites deployed Python *in
    place* — same target, same path, new implementation. A context that survived
    between orchestrations would find the old module in ``sys.modules`` and
    never look at the file again, so the load after a rebuild would run the code
    the load before it ran.

    .. code-block:: text

        run 1    imports Sales__Customer, version A
        build    rewrites the same path with version B
        run 2    must execute version B
    """

    root = tree(tmp_path, "raw")
    deployed = root / "Sales__Customer.py"
    deployed.write_text("class Sales__Customer:\n    version = 'A'\n", encoding="utf-8")

    with RuntimeScope.new() as first_run:
        first = _customer(
            first_run.context_for(
                logical_item="Lakehouse/Raw",
                physical_target="Lakehouse/Raw_LH",
                runtime_root=root,
            )
        )
        assert first.Sales__Customer.version == "A"

    # The build, rewriting the same physical path.
    deployed.write_text("class Sales__Customer:\n    version = 'B'\n", encoding="utf-8")

    with RuntimeScope.new() as second_run:
        second = _customer(
            second_run.context_for(
                logical_item="Lakehouse/Raw",
                physical_target="Lakehouse/Raw_LH",
                runtime_root=root,
            )
        )

        assert second.Sales__Customer.version == "B"


@weaver_test()
def test_closing_a_scope_leaves_nothing_of_it_behind(tmp_path):
    """The namespace goes whole, because deciding what may be kept would mean
    knowing whether a build has rewritten it — which nothing here can see."""

    with RuntimeScope.new() as scope:
        context = scope.context_for(
            logical_item="Lakehouse/Raw",
            physical_target="Lakehouse/Raw_LH",
            runtime_root=tree(tmp_path, "raw"),
        )
        _customer(context)
        assert any(name.startswith(context.package) for name in sys.modules)

    assert not [name for name in sys.modules if name.startswith(context.package)]


@weaver_test()
def test_two_runs_never_share_a_context_identity(tmp_path):
    """Which is why nothing has to detect staleness: the name is never reused."""

    root = tree(tmp_path, "raw")
    ids = []
    for _ in range(2):
        with RuntimeScope.new() as scope:
            ids.append(
                scope.context_for(
                    logical_item="Lakehouse/Raw",
                    physical_target="Lakehouse/Raw_LH",
                    runtime_root=root,
                ).context_id
            )

    assert ids[0] != ids[1]


# --- the context name ---------------------------------------------------------


@weaver_test()
def test_a_context_identity_is_opaque_rather_than_derived_from_weaver_names(raw):
    """Deriving it from Weaver names means normalising them, and normalisation
    is not injective: two distinct valid identities could reduce to one Python
    package and then quietly share modules. A UUID cannot."""

    assert raw.context_id.startswith("c")
    assert "raw" not in raw.context_id.lower()
    assert raw.package == f"{ROOT_PACKAGE}.{raw.context_id}"


@weaver_test()
def test_one_item_in_one_target_is_one_context_within_a_run(scope, tmp_path):
    root = tree(tmp_path, "raw")
    first = scope.context_for(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=root,
    )
    second = scope.context_for(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=root,
    )

    assert first is second


@weaver_test()
def test_two_targets_of_one_item_are_two_contexts(scope, tmp_path):
    """The same repository built into two Lakehouses is two estates."""

    first = scope.context_for(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Raw_LH",
        runtime_root=tree(tmp_path / "a", "raw"),
    )
    second = scope.context_for(
        logical_item="Lakehouse/Raw",
        physical_target="Lakehouse/Other_LH",
        runtime_root=tree(tmp_path / "b", "other"),
    )

    assert first.context_id != second.context_id
