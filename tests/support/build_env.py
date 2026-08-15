"""The build environment a test drives, independent of transport.

`BuildEnv` hides *how* a build reaches its estate behind callables, so a test
body names what it is asserting rather than Livy, Spark or ODBC. It lives here
rather than in `tests/fabric/conftest.py` so that a test importing it does not
thereby acquire a workspace, a credential and a session.

`tests/fabric/conftest.py` supplies the callables.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from weaver.store import Store
from weaver.targets import ItemRef

from .observation import Observation, observation_from, observe_body
from .workspaces import WORKSPACE

if TYPE_CHECKING:  # names used only in annotations
    from weaver.workspaces import Workspace


@dataclass(frozen=True)
class PopulatedLakehouse:
    """One populated target, with transport hidden from the shared test."""

    workspace: Workspace
    target: ItemRef
    resolver: Any
    store: Store
    wipe: Callable[[], tuple[str, ...]]


@dataclass
class InstalledEstate:
    """One estate provisioned and installed once, for read-only assertions.

    A test that rebuilds — to exercise prune, say — calls ``env.generate()``
    again; the bindings come from the environment's fixture, so nothing has to
    be named twice.
    """

    env: "BuildEnv"
    bundle: Any


@dataclass
class Step:
    """One transition of a journey, and what it produced.

    Assertions read this rather than doing the work themselves, which is what
    lets a dozen checks share one build.
    """

    name: str
    bundle: Any = None
    outcome: "InstallOutcome | None" = None
    #: Set when the transition raised. Later steps then report *this* name, so a
    #: journey fails once and says where.
    error: BaseException | None = None
    #: The one evidence payload taken at this transition, set by the test that
    #: asserts it. Kept on the step so an assertion names the moment it is about:
    #: the estate itself has moved on by the time the next transition finishes.
    observation: "Observation | None" = None

    @property
    def actions(self) -> dict:
        """Planned action kind by action id, for order and presence assertions."""

        return {
            action.id: action.kind
            for _sequence, _batch, action in self.bundle.plan.actions()
        }

    @property
    def sequence_of(self) -> dict:
        """Which barrier each action landed in, for ordering assertions."""

        return {
            action.id: sequence.number
            for sequence, _batch, action in self.bundle.plan.actions()
        }

    def kinds(self) -> set:
        return set(self.actions.values())


class Journey:
    """One estate driven through an ordered series of transitions.

    The suite's cost is *estates*, not assertions: a module-scoped Fabric estate
    is one full generate-and-install, and six checks over it cost exactly what
    one does. The old shape therefore paid an install per module and could only
    ever ask "what did a first build do?" — which is the question the fast suite
    already answers, and not the one incremental logic lives in.

    A journey inverts that. It installs once, then *moves* the estate — change a
    document, seed an orphan, break a payload, wipe — and each move costs one
    round trip while every assertion about it costs nothing. So it is both
    cheaper and able to ask the questions that matter: what did the *second*
    build do, and did it correctly do nothing?

    **A journey owns its state.** Its own repository root, its own logical item
    names, its own physical targets. Estates in one run otherwise collide through
    shared things — repository source locations, the Registry (which is keyed by
    logical item), and the fixed Lakehouses. Each of those has already produced
    a confusing failure, so every environment carries its source explicitly.

    **A failed transition does not cascade.** The step records the exception and
    every later step is skipped with its name, so a broken journey reports one
    failure naming the move that broke rather than a screen of errors whose cause
    is the first of them.
    """

    def __init__(self, env: "BuildEnv", name: str) -> None:
        self.env = env
        self.name = name
        self.steps: dict[str, Step] = {}
        self._failed: str | None = None

    def run(self, name: str, *, before=None, between=None) -> Step:
        """Take one transition: optionally change something, then build.

        ``before`` mutates the repository or the target — it is the *move*, and
        the build that follows is what the assertions are about.

        ``between(env, bundle)`` runs after generation and before installation,
        for the two claims that need the world to change mid-transition. It may
        return a bundle to install *instead* of the generated one:

        - removing the source repository, to prove a bundle installs from itself
        - substituting a corrupted bundle, to prove a failing action stops its
          barrier

        A transition whose installation reports failure is recorded in full — the
        step keeps its bundle and outcome, so the test that expects a failure can
        inspect it — but the journey is marked failed, so anything after it is
        skipped rather than asserting against a part-built estate.
        """

        if self._failed is not None:
            step = Step(
                name=name, error=RuntimeError(f"upstream step {self._failed!r} failed")
            )
            self.steps[name] = step
            return step
        try:
            if before is not None:
                before(self.env)
            bundle = self.env.generate(f"{self.name}-{name}")
            if between is not None:
                bundle = between(self.env, bundle) or bundle
            outcome = self.env.install(bundle)
            step = Step(name=name, bundle=bundle, outcome=outcome)
            if outcome.status != "succeeded":
                self._failed = name
        except BaseException as exc:  # recorded, not raised: the journey continues
            self._failed = name
            step = Step(name=name, error=exc)
        self.steps[name] = step
        return step

    def __getitem__(self, name: str) -> Step:
        step = self.steps[name]
        if step.error is not None:
            raise AssertionError(f"journey step {name!r} failed: {step.error}")
        return step


@dataclass
class InstallOutcome:
    """An environment-neutral view of an installation report."""

    status: str
    bundle_id: str
    sequence_status: dict[int, str]
    action_status: dict[str, str]
    action_order: tuple[str, ...]
    action_error: dict[str, str] = None  # action_id -> "Type: message", failures only


@dataclass
class BuildEnv:
    """Everything a build test needs, with transport hidden behind callables.

    Assertions are written in logical names — ``{{object:DWG.Customer}}`` — and
    resolved against a named destination before they run. That is not sugar. A
    test that asked for ``DWG.Customer`` would resolve it through the session's
    own catalogue, which is exactly the mistake the build no longer makes: it
    would read back from the Lakehouse the object was wrongly written to and
    pass. Naming the destination in the assertion is what makes the assertion
    able to see the thing it claims about.

    The substitution is this harness's own. A *payload* carries no tokens any
    more — a build renders final names — so what is resolved here is only the
    shorthand a test is written in.
    """

    label: str
    workspace: Any
    weaver: ItemRef
    target: ItemRef
    resolver: Any
    store: Store
    repository_root: Any
    generate_spark: Any
    #: Install this env's explicit repository source, replacing whatever was
    #: there. The fixture chooses the content and the bindings.
    install_repo: Callable[[], None]
    remove_repo: Callable[[], None]
    generate: Callable[..., Any]
    install: Callable[[Any], InstallOutcome]
    #: Raw SQL, run wherever this environment runs. Prefer ``query``.
    run_query: Callable[[str], list]
    #: ``[{"name", "type", "nullable"}]`` for a table — schema with nullability,
    #: which ``query``/DESCRIBE cannot give. Warehouse reads it from the catalogue.
    run_columns: Callable[[str], list]
    seed_orphans: Callable[[], None]
    #: Whether a fully-qualified schema exists. Asked rather than listed, because
    #: an absent schema is the answer a prune assertion wants and both workspaces raise
    #: for `SHOW TABLES` in one.
    run_schema_exists: Callable[[str], bool] = None
    #: Python source, run wherever this environment runs, returning whatever it
    #: ``emit``s. The namespace carries ``spark``, ``resolver`` and ``target``, so
    #: one body serves both transports — the only way to exercise code that must
    #: behave identically in a notebook and on a laptop.
    run_python: Callable[[str], Any] = None
    #: The destination Lakehouse being built. The catalogue is a Warehouse and
    #: is reached over TDS, so it has no Spark destination to carry here.
    destination: Any = None
    #: Every Lakehouse this fixture bound, by the item that owns it. Empty unless
    #: the fixture asked for more than one — a cross-item alias is the only thing
    #: that does, and it needs both ends addressable to prove the alias points
    #: across rather than at itself.
    destinations: Mapping[str, Any] = field(default_factory=dict)

    def at(self, destination=None):
        return destination or self.destination

    def _addressed(self, text: str, destination) -> str:
        """Resolve this harness's shorthand against a named Spark destination.

        A Warehouse environment has none — it is reached over TDS and its names
        are ordinary T-SQL — so its statements pass through untouched.
        """

        place = self.at(destination)
        if place is None:
            return text
        text = re.sub(
            r"\{\{object:([^.{}]+)\.([^.{}]+)\}\}",
            lambda match: place.qualify(match.group(1), match.group(2)),
            text,
        )
        text = re.sub(
            r"\{\{schema:([^.{}]+)\}\}",
            lambda match: place.qualified_schema(match.group(1)),
            text,
        )
        left = re.search(r"\{\{[^{}]*\}\}", text)
        if left:
            raise AssertionError(
                f"{left.group(0)} is not a name this harness resolves; a test "
                "writes {{object:Schema.Name}} or {{schema:Name}}"
            )
        return text

    def query(self, sql: str, *, destination=None) -> list:
        """Run a query, resolving its object tokens against one destination.

        One round trip per call. Prefer :meth:`observe` for anything asking more
        than one question of the same estate at the same moment.
        """

        return self.run_query(self._addressed(sql, destination))

    def observe(
        self, queries=None, schemas=None, tables=None, *, label="observe"
    ) -> Observation:
        """Ask the estate everything at once and bring back one evidence payload.

        A Fabric round trip costs seconds, so the number of them — not the work
        inside them — sets what this suite costs. Six ``query`` calls describing
        one moment are six waits for the same answer; this submits their bodies
        together and returns one payload, which the test then asserts against
        in this process.

        That is not only cheaper, it is more accurate. Separate calls interrogate
        a *mutable remote estate* at six different instants, so a claim about
        "the estate after prune" is really six claims about six moments. One
        payload is one observation of one moment, which is what the assertion
        says it is.

        ``queries``, ``schemas`` and ``tables`` are mappings of evidence name to,
        respectively, a statement, a schema name and a ``Schema.Object`` pair. A
        value may instead be a ``(text, destination)`` pair, so one observation
        can span the destination Lakehouse *and* the control plane — the pairing
        that proves a build wrote where it claimed and nowhere else, and which
        two calls could never make about the same instant.

        Ask ``tables`` rather than ``queries`` where *absent* is a legitimate
        answer: a SELECT against a missing table raises instead of reporting.

        Failures stay local: :class:`Observation` names the piece of evidence
        that disappointed, rather than a traceback from inside a Spark session.
        """

        addressed = {}
        for name, probe in (queries or {}).items():
            text, destination = self._probe(probe)
            addressed[name] = self._addressed(text, destination)

        wanted_schemas = {}
        for name, probe in (schemas or {}).items():
            schema, destination = self._probe(probe)
            wanted_schemas[name] = self.schema_name(schema, destination=destination)

        wanted_tables = {}
        for name, probe in (tables or {}).items():
            qualified, destination = self._probe(probe)
            schema, _, obj = qualified.partition(".")
            wanted_tables[name] = self.name(schema, obj, destination=destination)

        # A Warehouse environment has no Spark session to batch into: it is
        # reached over TDS, where a statement is a cheap local round trip and not
        # a Livy submission. Batching there would buy nothing and hide the shape
        # of what ran, so it stays a loop — and the test-facing API is the same
        # either way, which is what lets one journey run against both.
        if self.run_python is None:
            return Observation(
                rows={name: self.run_query(sql) for name, sql in addressed.items()},
                schemas={
                    name: self.run_schema_exists(sql)
                    for name, sql in wanted_schemas.items()
                },
            )
        return observation_from(
            self.run_python(
                observe_body(addressed, wanted_schemas, wanted_tables), label=label
            )
        )

    def _probe(self, value) -> tuple[str, Any]:
        """A probe is ``text`` against the default destination, or ``(text, dest)``."""

        if isinstance(value, tuple):
            return value
        return (value, None)

    def columns(self, table: str, *, destination=None) -> list:
        return self.run_columns(self._addressed(table, destination))

    def name(self, schema: str, obj: str, *, destination=None) -> str:
        return self.at(destination).qualify(schema, obj)

    def schema_name(self, schema: str, *, destination=None) -> str:
        return self.at(destination).qualified_schema(schema)

    def schema_exists(self, schema: str, *, destination=None) -> bool:
        return self.run_schema_exists(self.schema_name(schema, destination=destination))

    def write_repo_file(self, relative: str, content: str) -> None:
        """Change the installed declaration between two builds.

        Incremental behaviour can only be asserted across builds, and what
        changes between them is the repository — so a test needs to edit it in
        place rather than install a second fixture. Both transports write through
        the same store abstraction, so this needs no per-environment form.
        """

        self.store.write(
            self.repository_root.join(*relative.split("/")),
            content.encode("utf-8"),
        )


def _outcome_from_report(report) -> InstallOutcome:
    return InstallOutcome(
        status=report.status,
        bundle_id=report.bundle_id,
        sequence_status={s.number: s.status for s in report.sequences},
        action_status={a.action_id: a.status for a in report.action_results()},
        action_order=tuple(a.action_id for a in report.action_results()),
        action_error={
            a.action_id: f"{a.error_type}: {a.error_message}"
            for a in report.action_results()
            if a.error_type
        },
    )


def _upload_tree(store, source: Path, destination) -> None:
    """Install a repository, *replacing* whatever was there under that name.

    Replacing, not merging. Two modules install different fixtures under the same
    repository name into one shared Weaver Lakehouse, and a plain file-by-file
    write left the previous fixture's objects behind — so a Warehouse estate
    inherited a Lakehouse-reading table from a repository it had never heard of,
    and failed on a three-part name naming a Lakehouse that does not exist here.
    Installing a repository has never meant "add to whatever is already called
    that".
    """

    try:
        store.delete(destination, recursive=True)
    except Exception:  # nothing there yet, which is the ordinary case
        pass
    for path in sorted(source.rglob("*")):
        if path.is_file():
            store.write(
                destination.join(*path.relative_to(source).parts), path.read_bytes()
            )


def _bindings_for(
    weaver_repo_fixture, *, lakehouse=None, warehouse=None, lakehouses=None
):
    """Bind the fixture's declared items to whichever targets this env has.

    The item type chooses the binding, so one environment serves a Lakehouse
    fixture, a Warehouse fixture or a mixed one without a test naming a target
    kind. Items the fixture does not list stay unbound, which is how the mixed
    estate proves its Warehouse leaves are omitted.

    ``lakehouses`` maps a specific item to its own Lakehouse, for the one thing a
    single destination cannot express: a cross-item alias needs the producer and
    the consumer in *different* Lakehouses, or the alias would point a name at
    something already in the same place. ``lakehouse`` remains the default for
    every item not named there, so single-target fixtures are untouched.
    """

    from weaver.build_bundle import (
        ItemBinding,
        ItemBindings,
        LakehouseBinding,
        WarehouseBinding,
    )
    from weaver.declaration.model import LAKEHOUSE, WeaverItemId

    by_item = {
        WeaverItemId.parse(name): ref for name, ref in (lakehouses or {}).items()
    }
    entries = []
    for name in weaver_repo_fixture.items:
        item = WeaverItemId.parse(name)
        if item.item_type == LAKEHOUSE:
            bound = by_item.get(item, lakehouse)
            if bound is None:
                raise AssertionError(f"{item} needs a Lakehouse this env does not have")
            entries.append(
                ItemBinding(
                    item,
                    LakehouseBinding(lakehouse=bound, workspace_name=WORKSPACE),
                )
            )
        else:
            if warehouse is None:
                raise AssertionError(f"{item} needs a Warehouse this env does not have")
            entries.append(
                ItemBinding(
                    item,
                    WarehouseBinding(warehouse=warehouse, workspace_name=WORKSPACE),
                )
            )
    return ItemBindings(tuple(entries))


#: Every schema any build fixture registers, in either Lakehouse. Dropped on
#: local-env teardown, so a shared Spark catalogue never leaks one test's objects
#: into the next — the one place catalogue cleanup lives; tests never do it
#: themselves. They are dropped through the destination, because a local schema's
#: real database name carries the Lakehouse it belongs to.


def _install_estate(env) -> InstalledEstate:
    """Install one estate through a BuildEnv, once, and assert it succeeded."""

    env.install_repo()
    bundle = env.generate()
    outcome = env.install(bundle)
    assert outcome.status == "succeeded", outcome.action_error
    return InstalledEstate(env=env, bundle=bundle)
