"""Importing one estate's deployed modules without meeting another's.

Authored code says what it has always said::

    from lib.dates import parse_date
    from Files.Sales__Seed import Sales__Seed

Those names are *process-global*. Two Lakehouses may each deploy a
``lib/dates.py``, and Python consults ``sys.modules`` before it searches
anything — so putting a different tree on ``sys.path`` does not help: the second
import finds the first tree's module and returns it, silently, with no error
anywhere. One estate per process is the normal case and an orchestrated run is
not it.

**The context has to be part of the key, not merely of the search.** So every
deployed module is imported under a name that carries the tree it came from:

.. code-block:: text

    lib.dates          ->  _weaver_runtime.lakehouse_raw__raw_lh.lib.dates
    Files.Sales__Seed  ->  _weaver_runtime.lakehouse_raw__raw_lh.Files.Sales__Seed

    the same file in another target
                       ->  _weaver_runtime.lakehouse_curated__curated_lh.lib.dates

Two entries, two modules, no collision — and both importable at once, which a
scheme that deleted ``lib.*`` between loads could not offer. That scheme would
also need a global lock and would make parallel dispatch impossible.

**Nothing about the authored surface changes.** The rewriting happens in one
place: each deployed module executes with its own ``__import__``, which redirects
exactly the names its own tree defines and passes everything else — ``weaver``,
``pyspark``, ``json`` — straight through. A module outside the runtime tree is
untouched, so a developer importing an object by hand in a notebook gets
ordinary Python.

**One context per logical item and physical target.** Objects deployed together
share a tree, because they were authored to: ``Sales__Customer`` and
``Sales__Order`` in one item read the same ``lib/dates``, and giving them
separate copies would break the sharing the author intended. Different targets
share nothing, which is the whole point.
"""

from __future__ import annotations

import builtins
import importlib
import sys
import threading
from dataclasses import dataclass, field
from importlib.machinery import ModuleSpec, PathFinder
from pathlib import Path

from ..errors import LoadError

#: The synthetic package every runtime tree hangs under. Leading underscore
#: because it is Weaver's, and no authored name can collide with it: an item that
#: deployed a top-level ``_weaver_runtime`` would have had to declare a schema
#: Weaver already reserves.
ROOT_PACKAGE = "_weaver_runtime"


@dataclass(frozen=True)
class PythonRuntimeContext:
    """One deployed runtime tree, and the package name it is imported under."""

    context_id: str
    runtime_root: Path
    #: The top-level names this tree defines, and therefore the only names an
    #: authored import may have meant *this* tree by. Frozen at construction:
    #: the tree is deployed by a build and does not change during a load.
    top_level: frozenset = field(default_factory=frozenset)

    @property
    def package(self) -> str:
        return f"{ROOT_PACKAGE}.{self.context_id}"

    def qualified(self, name: str) -> str:
        """``lib.dates`` as this context imports it."""

        return f"{self.package}.{name}"


def runtime_context(
    *, logical_item, physical_target, runtime_root: str | Path
) -> PythonRuntimeContext:
    """The context one node's module is imported under.

    Derived from values the resolved node already carries, so nothing has to be
    threaded through or looked up — and deterministic, so the same node in the
    same run reaches the same modules however many times it is dispatched.
    """

    root = Path(str(runtime_root))
    return PythonRuntimeContext(
        context_id=_identifier(f"{logical_item}__{physical_target}"),
        runtime_root=root,
        top_level=_top_level_names(root),
    )


def import_deployed_module(
    context: PythonRuntimeContext, relative: str, *, expected: str, node_id: str
):
    """One deployed module, imported inside its context, with its class present.

    ``relative`` is the module's path within the tree — ``Files/Sales__Seed.py``
    — which is also what names it: a module is imported as its *position*, so
    the name another deployed module imports it by is the name it is registered
    under, and one file never becomes two module objects.
    """

    _install_finder()
    _FINDER.register(context)

    dotted = relative[: -len(".py")] if relative.endswith(".py") else relative
    name = context.qualified(dotted.replace("/", "."))
    try:
        module = importlib.import_module(name)
    except LoadError:
        raise
    except ModuleNotFoundError as exc:
        # Which module was missing decides which failure this is. The one asked
        # for means nothing was deployed there; anything else means the module
        # was found and its *own* import failed — a different fault, with a
        # different fix, and reporting them alike sends the reader to the wrong
        # place.
        missing = exc.name or ""
        if missing and (name == missing or name.startswith(f"{missing}.")):
            raise LoadError(
                f"{node_id}: no deployed module at "
                f"{context.runtime_root}/{relative}"
            ) from exc
        raise LoadError(
            f"{node_id}: importing {context.runtime_root}/{relative} raised "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    except FileNotFoundError as exc:
        raise LoadError(
            f"{node_id}: no deployed module at "
            f"{context.runtime_root}/{relative}"
        ) from exc
    except Exception as exc:  # noqa: BLE001 - authored code, any failure is data
        raise LoadError(
            f"{node_id}: importing {context.runtime_root}/{relative} raised "
            f"{type(exc).__name__}: {exc}"
        ) from exc
    if not hasattr(module, expected):
        raise LoadError(
            f"{node_id}: {context.runtime_root}/{relative} defines no class "
            f"{expected!r} — a deployed object module names its class for its file"
        )
    return module


def forget(context: PythonRuntimeContext) -> None:
    """Drop one context's modules. For tests; a run never needs it.

    A load leaves its context in place deliberately — a second load of the same
    target reuses it, which is what makes repeated dispatch cheap and what keeps
    a module's identity stable across a run.
    """

    _forget_modules(context.package)
    _FINDER.contexts.pop(context.package, None)
    _BUILTINS.pop(context.package, None)


def _forget_modules(package: str) -> None:
    prefix = f"{package}."
    for name in [
        name for name in sys.modules if name == package or name.startswith(prefix)
    ]:
        del sys.modules[name]


# --- the import machinery -----------------------------------------------------


def _context_builtins(context: PythonRuntimeContext) -> dict:
    """A builtins mapping whose ``__import__`` knows one tree.

    Every deployed module executes with this in place of the real builtins, so
    the redirection reaches exactly the modules Weaver deployed and nothing
    else. A name the tree does not define — ``weaver``, ``pyspark``, ``json`` —
    goes to the ordinary import, unchanged.
    """

    real = builtins.__import__

    def importer(name, globals=None, locals=None, fromlist=(), level=0):
        if level == 0 and name:
            head = name.split(".", 1)[0]
            if head in context.top_level:
                qualified = context.qualified(name)
                importlib.import_module(qualified)
                # ``import lib.dates`` binds the *top* name; ``from lib.dates
                # import x`` wants the module itself. The real __import__ makes
                # the same distinction, and callers rely on it.
                return sys.modules[
                    qualified if fromlist else context.qualified(head)
                ]
        return real(name, globals, locals, fromlist or (), level)

    return {**vars(builtins), "__import__": importer}


class _ContextLoader:
    """A loader that runs a deployed module with its context's builtins."""

    def __init__(self, inner, context: PythonRuntimeContext) -> None:
        self._inner = inner
        self._context = context

    def create_module(self, spec):
        return self._inner.create_module(spec)

    def exec_module(self, module):
        # Before execution, because the module's own imports run during it.
        module.__dict__["__builtins__"] = _BUILTINS[self._context.package]
        self._inner.exec_module(module)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _WeaverRuntimeFinder:
    """Resolves ``_weaver_runtime.<context>.…`` against that context's tree."""

    def __init__(self) -> None:
        self.contexts: dict[str, PythonRuntimeContext] = {}
        self._lock = threading.Lock()

    def register(self, context: PythonRuntimeContext) -> None:
        """Bind a context, forgetting what it held if its tree has moved.

        One item in one target is one tree, so within a session the root for a
        given context does not change — the Lakehouse it resolves to is the same
        Lakehouse. When it *does* change, the tree has been redeployed
        somewhere else and the modules already imported describe the old one.
        Rebinding and dropping them is the only answer that cannot serve stale
        code; raising would refuse work that is perfectly valid, and keeping the
        old root would quietly load the wrong estate.
        """

        with self._lock:
            known = self.contexts.get(context.package)
            moved = known is not None and known.runtime_root != context.runtime_root
            self.contexts[context.package] = context
            if moved:
                _forget_modules(context.package)
                _BUILTINS.pop(context.package, None)
            _BUILTINS.setdefault(context.package, _context_builtins(context))

    def find_spec(self, fullname, path=None, target=None):
        if fullname == ROOT_PACKAGE:
            # A namespace package holding the contexts, and nothing else.
            return ModuleSpec(fullname, None, is_package=True)
        context = self._owner(fullname)
        if context is None:
            return None
        if fullname == context.package:
            spec = ModuleSpec(fullname, None, is_package=True)
            spec.submodule_search_locations = [str(context.runtime_root)]
            return spec
        spec = PathFinder.find_spec(fullname, path)
        if spec is None:
            return None
        if spec.loader is None:
            # A directory with no ``__init__.py`` — ``lib/``, ``Files/``. There
            # is nothing to execute, so there is nothing to give builtins to.
            return spec
        spec.loader = _ContextLoader(spec.loader, context)
        return spec

    def _owner(self, fullname: str) -> PythonRuntimeContext | None:
        for package, context in self.contexts.items():
            if fullname == package or fullname.startswith(f"{package}."):
                return context
        return None


#: One finder for every context, so ``sys.meta_path`` gains a single entry
#: however many trees a process holds.
_FINDER = _WeaverRuntimeFinder()
_BUILTINS: dict[str, dict] = {}
_INSTALLED = threading.Lock()


def _install_finder() -> None:
    with _INSTALLED:
        if _FINDER not in sys.meta_path:
            # Ahead of the ordinary finders: ``_weaver_runtime`` is Weaver's
            # namespace and must not be resolved against sys.path.
            sys.meta_path.insert(0, _FINDER)


def _top_level_names(root: Path) -> frozenset:
    """What this tree defines at its root — the redirectable names."""

    if not root.is_dir():
        return frozenset()
    names = set()
    for entry in root.iterdir():
        if entry.is_dir() and not entry.name.startswith("__"):
            names.add(entry.name)
        elif entry.suffix == ".py" and not entry.name.startswith("__"):
            names.add(entry.stem)
    return frozenset(names)


def _identifier(value: str) -> str:
    """A safe, deterministic package name for one item-and-target pair.

    Lower-cased and reduced to word characters, because it becomes a Python
    identifier — and deterministic rather than hashed, because a name in a
    traceback should say which estate it came from.
    """

    cleaned = "".join(
        character if character.isalnum() or character == "_" else "_"
        for character in str(value)
    ).strip("_")
    while "___" in cleaned:
        cleaned = cleaned.replace("___", "__")
    return cleaned.lower() or "unnamed"


__all__ = [
    "ROOT_PACKAGE",
    "PythonRuntimeContext",
    "forget",
    "import_deployed_module",
    "runtime_context",
]
