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

    lib.dates          ->  _weaver_runtime.c7f3e1….lib.dates
    Files.Sales__Seed  ->  _weaver_runtime.c7f3e1….Files.Sales__Seed

    the same file in another target
                       ->  _weaver_runtime.c91a04….lib.dates

Two entries, two modules, no collision — and both importable at once, which a
scheme that deleted ``lib.*`` between loads could not offer. That scheme would
also need a global lock and would make parallel dispatch impossible.

**The identity is opaque, and it lasts one run.** Both properties are there to
stop the module table telling a lie.

*Opaque*, because deriving the package name from Weaver names means normalising
them, and normalisation is not injective: two distinct valid identities can
reduce to one Python package and then quietly share modules. A UUID cannot.

*One run*, because a deployed file is not immutable. An ordinary rebuild
rewrites ``Files/_/Load/Sales__Customer.py`` in place, and a Fabric session
outlives it:

.. code-block:: text

    load 1   imports Sales__Customer, version A
    build    rewrites the same path with version B
    load 2   must execute version B

A context that survived between orchestrations would find version A in
``sys.modules`` and never look at the file. So each :class:`RuntimeScope` mints
fresh ids and drops its whole namespace when the run ends — no
deployment-generation tracking, no staleness to detect, because nothing is
carried across the boundary in the first place.

**Nothing about the authored surface changes.** The rewriting happens in one
place: each deployed module executes with its own ``__import__``, which redirects
exactly the names its own tree defines and passes everything else — ``weaver``,
``pyspark``, ``json`` — straight through. A module outside the runtime tree is
untouched, so a developer importing an object by hand in a notebook gets
ordinary Python.

**One context per logical item and physical target, within a run.** Objects
deployed together share a tree, because they were authored to:
``Sales__Customer`` and ``Sales__Order`` in one item read the same
``lib/dates``, and giving them separate copies would break the sharing the
author intended. Different targets share nothing, which is the whole point.
"""

from __future__ import annotations

import builtins
import importlib
import sys
import threading
import uuid
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


class RuntimeScope:
    """One ``weaver.load()``'s worth of runtime contexts.

    Created per run, and torn down when the run ends. Within it, a logical item
    in a physical target maps to one context however many of its objects are
    dispatched — so they share their ``lib/`` and ``Files/`` modules, as their
    author wrote them to. Across runs nothing is shared at all, which is what
    makes a rebuilt module take effect on the next load rather than the next
    session.
    """

    def __init__(self) -> None:
        self._contexts: dict[tuple[str, str], PythonRuntimeContext] = {}
        self._lock = threading.Lock()

    @classmethod
    def new(cls) -> "RuntimeScope":
        return cls()

    def context_for(
        self, *, logical_item, physical_target, runtime_root: str | Path
    ) -> PythonRuntimeContext:
        """This run's context for one item deployed into one target."""

        key = (str(logical_item), str(physical_target))
        with self._lock:
            known = self._contexts.get(key)
            if known is not None:
                return known
            root = Path(str(runtime_root))
            context = PythonRuntimeContext(
                # Opaque, and unrelated to any Weaver name: normalising an
                # identity into a Python package name is not injective, and two
                # distinct items that normalised alike would share modules.
                context_id=f"c{uuid.uuid4().hex}",
                runtime_root=root,
                top_level=_top_level_names(root),
            )
            self._contexts[key] = context
            return context

    def close(self) -> None:
        """Drop every module this run imported.

        The whole namespace goes, because the alternative is deciding *which*
        modules a later run may keep — and the answer depends on whether a build
        has rewritten them since, which nothing here can see.
        """

        with self._lock:
            contexts = list(self._contexts.values())
            self._contexts.clear()
        for context in contexts:
            forget(context)

    def __enter__(self) -> "RuntimeScope":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False


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
    # A build may have added files since anything last looked at this directory,
    # and the finder caches directory listings per path.
    importlib.invalidate_caches()

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
    """Drop one context's modules, its finder entry and its builtins.

    Called by :meth:`RuntimeScope.close` when a run ends. Nothing survives it:
    the package name is never minted again, so anything left behind would be
    unreachable memory rather than a cache.
    """

    _forget_modules(context.package)
    _FINDER.unregister(context.package)


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
                # Qualify the name and then hand it to the *real* import.
                # Anything less reimplements Python's import semantics, and the
                # part most easily missed is `fromlist`: ``from lib import
                # dates`` calls this with name="lib" and fromlist=("dates",),
                # and it is the real machinery that then loads the submodule and
                # attaches it to its package. Returning the package unchanged
                # leaves ``dates`` never imported, and the import fails on a
                # name that is perfectly valid Python.
                qualified = context.qualified(name)
                imported = real(qualified, globals, locals, fromlist or (), level)
                if fromlist:
                    return imported
                # Without a fromlist, ``__import__`` returns the *root* package —
                # ``import lib.dates`` binds ``lib``. The real call returned
                # ``_weaver_runtime``, which is Weaver's root and not the
                # author's, so the redirected head is what belongs here.
                return sys.modules[context.qualified(head)]
        return real(name, globals, locals, fromlist or (), level)

    return {**vars(builtins), "__import__": importer}


class _ContextLoader:
    """Runs a deployed module with its context's builtins, from source.

    **From source, and that is not an optimisation in reverse.** Python caches
    compiled bytecode beside the file and decides whether the cache is current
    from the source's size and its mtime *to the second*. A deployed module is
    rewritten in place by builds, so a rebuild that changes a module without
    changing its length — the generated ``SparkSqlTable`` wrapper, whose length
    moves only if the embedded SQL does — can land inside the same second and be
    indistinguishable from no change at all. The next load then runs the
    previous build's code, silently.

    Compiling from source each time removes the question. It also stops
    ``__pycache__`` directories appearing inside the deployed tree, which in
    Fabric means inside OneLake, inside a folder Weaver manages and prunes.

    The cost is a compile per module per run, against modules that are a few
    dozen lines and a load that is about to touch a warehouse.
    """

    def __init__(self, inner, builtins_mapping: dict) -> None:
        self._inner = inner
        self._builtins = builtins_mapping

    def create_module(self, spec):
        return self._inner.create_module(spec)

    def get_code(self, fullname):
        source = self._inner.get_source(fullname)
        if source is None:  # pragma: no cover - a namespace or extension module
            return self._inner.get_code(fullname)
        return compile(source, self._inner.get_filename(fullname), "exec")

    def exec_module(self, module):
        code = self.get_code(module.__name__)
        # Before execution, because the module's own imports run during it.
        module.__dict__["__builtins__"] = self._builtins
        exec(code, module.__dict__)  # noqa: S102 - deployed Weaver source

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _WeaverRuntimeFinder:
    """Resolves ``_weaver_runtime.<context>.…`` against that context's tree.

    **Every read and write of the registry is under one lock, and no read walks
    it.** Loads are sequential today and the design exists so they need not stay
    that way, so the registry has to survive one thread registering while
    another resolves. A scan would be both the slower answer and the unsafe one:
    a module's own name already carries its context, so the entry can be looked
    up directly.
    """

    def __init__(self) -> None:
        self._contexts: dict[str, _Bound] = {}
        self._lock = threading.Lock()

    def register(self, context: PythonRuntimeContext) -> None:
        """Bind a context so its package resolves against its own tree.

        No rebinding case to handle: an id is minted once, by one scope, and is
        never seen again — so a package name cannot come to mean a second tree,
        and there are no modules under it left over from anything earlier.
        """

        with self._lock:
            if context.package not in self._contexts:
                self._contexts[context.package] = _Bound(
                    context=context, builtins=_context_builtins(context)
                )

    def unregister(self, package: str) -> None:
        """Drop a context's binding. The counterpart of :meth:`register`."""

        with self._lock:
            self._contexts.pop(package, None)

    def find_spec(self, fullname, path=None, target=None):
        if fullname == ROOT_PACKAGE:
            # A namespace package holding the contexts, and nothing else.
            return ModuleSpec(fullname, None, is_package=True)
        bound = self._bound(fullname)
        if bound is None:
            return None
        context = bound.context
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
        spec.loader = _ContextLoader(spec.loader, bound.builtins)
        return spec

    def _bound(self, fullname: str) -> "_Bound | None":
        """The binding this module name belongs to, by direct lookup.

        ``_weaver_runtime.<context>.lib.dates`` names its own context in its
        second component, so there is nothing to search for.
        """

        parts = fullname.split(".", 2)
        if len(parts) < 2 or parts[0] != ROOT_PACKAGE:
            return None
        with self._lock:
            return self._contexts.get(f"{parts[0]}.{parts[1]}")


@dataclass(frozen=True)
class _Bound:
    """One registered context and the builtins its modules execute with.

    Held together so registering is one insertion under one lock, rather than
    two mappings that a reader could catch disagreeing.
    """

    context: PythonRuntimeContext
    builtins: dict


#: One finder for every context, so ``sys.meta_path`` gains a single entry
#: however many trees a process holds.
_FINDER = _WeaverRuntimeFinder()
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


__all__ = [
    "ROOT_PACKAGE",
    "PythonRuntimeContext",
    "RuntimeScope",
    "forget",
    "import_deployed_module",
]
