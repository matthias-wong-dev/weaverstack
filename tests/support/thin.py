"""A thin run: every real link in the chain, and deliberately nothing at the end.

.. code-block:: text

    Runner → dispatch → Session → real import → a trivial installed artefact

Everything in that chain is production code. The only thing not real is what the
primitive *does*, and that is the point: a thin run proves the wiring — that the
Runner reaches dispatch, that dispatch resolves the deployed artefact and reaches
it through the Session, and that whatever comes back is settled into the run's
own vocabulary. What a primitive would have done to data is a Primitive's claim,
and is proven against real engines elsewhere.

**No build, and no estate to speak of.** A run needs two things to reach a
primitive: a catalogue saying it is installed, and the artefact being where the
catalogue says. Both are arranged directly here. The Registry points at these
artefacts exactly as it points at production ones, which is what makes the
Runner unable to tell the difference — and it is why paying for a build to make
a primitive callable is unnecessary when the claim is about dispatch.

Each artefact exists to produce one controlled physical outcome, chosen because
each is settled by a different rule:

.. code-block:: text

    Success      a result reporting success
    Rejects      rows written and rows refused — tolerated, or not
    Failure      a reported failure, nothing raised
    Raises       an exception the primitive never normalised
    Malformed    something that is not a result at all

    Agrees       a validation finding nothing
    Disagrees    a validation finding a discrepancy on both sides
    Unreadable   a validation that could not be evaluated at all

Loads need no Spark session at all, because a trivial load never touches one —
so those run in the pure suite. A Test's artefact returns a frame, so the
judgements need a real one; they still build nothing and read nothing, and cost
milliseconds on a Spark session the suite has already paid for.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from factories import (
    installed_catalogue,
    installed_inventories,
    item_bindings,
    item_id,
    lakehouse_table,
    lakehouse_test,
    single_document_repository,
)
from support.sessions import given_session

from weaver.etl import item_runtime_artefacts
from weaver.load_plan import PhysicalTargetRef
from weaver.run import RunState
from weaver.store import FilesystemStore
from weaver.targets import ItemRef
from support.workspaces import given_resolver, given_workspace

#: The schema these artefacts are declared in, so a thin node is recognisable in
#: a report as trivial rather than mistaken for an estate someone cared about.
SCHEMA = "Thin"

#: One trivial artefact per outcome. The class name is the module name, which is
#: the rule the deployed tree already follows — so these are found the way any
#: deployed module is found, not by a special case.
ARTEFACTS = {
    "Success": '''\
from weaver.runtime.load_result import LoadResult


class {name}:
    """Succeeds, having done nothing. The wiring is the whole claim."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def load(self, fault_tolerant=False):
        return LoadResult(succeeded=True, rows_read=2, rows_inserted=2)
''',
    "Rejects": '''\
from weaver.errors import LoadError
from weaver.runtime.load_result import LoadResult


class {name}:
    """Refuses one row, and answers the way the real runtime answers.

    Both halves matter, and both are spelled as ``table_load`` spells them.
    A tolerated rejection *returns* a result marked failed that counted its
    rejects; an intolerant one *raises*, carrying the same counts. The two
    results are indistinguishable — the raising is the only difference, which
    is precisely why a fixture that invented a tidier spelling would prove
    the Runner settles something no primitive ever sends it.
    """

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def load(self, fault_tolerant=False):
        refused = LoadResult.failure(
            "1 row was rejected and this load tolerates none",
            rows_read=3,
            rows_rejected=1,
        )
        if not fault_tolerant:
            raise LoadError("Thin.Rejects: rows were rejected", result=refused)
        return LoadResult(
            succeeded=True, rows_read=3, rows_inserted=2, rows_rejected=1
        ).rejected("1 row was rejected and tolerated")
''',
    "Failure": '''\
from weaver.runtime.load_result import LoadResult


class {name}:
    """Reports failure in its own result, without raising."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def load(self, fault_tolerant=False):
        return LoadResult.failure("the source system said no")
''',
    "Raises": '''\
class {name}:
    """Raises something the primitive never normalised."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def load(self, fault_tolerant=False):
        raise RuntimeError("the source system was unreachable")
''',
    "Malformed": '''\
class {name}:
    """Returns something that cannot say whether it succeeded."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def load(self, fault_tolerant=False):
        return {{"rows": 4}}
''',
}

OUTCOMES = tuple(ARTEFACTS)



@dataclass(frozen=True)
class ThinEstate:
    """What a thin run needs, and nothing a build would have brought with it."""

    session: Any
    workspace: Any
    state: Any
    target: PhysicalTargetRef
    root: Path
    #: Node id by outcome, so a test names the outcome rather than spelling ids.
    nodes: dict = field(default_factory=dict)

    def node(self, outcome: str) -> str:
        return self.nodes[outcome]

    def result(self, report, outcome: str):
        """The one node result for an outcome, found by name rather than index."""

        node_id = self.nodes[outcome]
        for result in report.nodes:
            if result.node_id == node_id:
                return result
        raise AssertionError(f"{node_id} is not in {[n.node_id for n in report.nodes]}")

    def judgement(self, report, name: str):
        """The one validation node for a judgement.

        Found by the object it validates rather than by a node id, because a
        validation report is keyed by what a reader names — the logical id —
        and that is the vocabulary difference the projection exists to make.
        """

        qualified = f"{SCHEMA}.{name}"
        for node in report.nodes:
            if node.logical_id.endswith(qualified):
                return node
        raise AssertionError(
            f"{qualified} is not in {[node.logical_id for node in report.nodes]}"
        )


def thin_estate(
    root: Path,
    *,
    outcomes: tuple[str, ...] = OUTCOMES,
    lakehouse: str = "Thin_LH",
) -> ThinEstate:
    """A Session, a RunState and deployed artefacts for what was named.

    The catalogue is composed by the same production constructors that describe
    a real estate, so what a thin run plans against is the shape a build
    publishes. Only the artefacts it points at are trivial.

    A thin run reaches a primitive and settles what comes back; it never
    touches data. Tests are not built here, because a Test's artefact returns a
    Spark frame — see ``tests/fabric/test_validation_dispatch.py``.
    """

    documents = {
        f"{SCHEMA}__{name}.py": lakehouse_table(f"{SCHEMA}.{name}")
        for name in outcomes
    }
    repository = single_document_repository(
        root / "repository", schemas=(SCHEMA,), documents=documents
    )
    bindings = item_bindings(("Lakehouse/Sales", lakehouse))

    workspace = given_workspace(weaver_lakehouse="Weaver_LH")
    resolver = given_resolver(
        workspace=workspace, lakehouses=("Weaver_LH", lakehouse), root=root
    )

    # Written where the *build* would have written them, asked of the build's own
    # enumerator rather than assembled from a path this module believes in. A
    # load module and a Test module do not share a directory, and a fixture that
    # guessed would only be pinning the guess.
    files_root = Path(resolver.files_root(ItemRef(lakehouse)).value)
    sources = {
        f"{SCHEMA}__{name}": source
        for name, source in (
            *((name, ARTEFACTS[name]) for name in outcomes),
        )
    }
    for artefact in item_runtime_artefacts(repository, item=item_id("Lakehouse/Sales")):
        module = artefact.identity.object_id.object.removesuffix(".py")
        source = sources.get(module)
        if source is None:
            continue
        path = files_root / artefact.target_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(source.format(name=module), encoding="utf-8")

    target = PhysicalTargetRef("lakehouse", lakehouse)
    return ThinEstate(
        session=given_session(
            workspace=workspace,
            store=FilesystemStore(),
            resolver=resolver,
            # A thin run reaches its primitive in this process, which is what
            # makes it thin: the whole dispatch path runs and none of it costs
            # a crossing.
            executes_here=True,
        ),
        workspace=workspace,
        state=RunState(
            catalogue=installed_catalogue(repository, bindings),
            target_inventories=installed_inventories(repository, bindings),
        ),
        target=target,
        root=root,
        nodes={name: f"load:{target}/{SCHEMA}.{name}" for name in outcomes},
    )


__all__ = ["ARTEFACTS", "OUTCOMES", "SCHEMA", "ThinEstate", "thin_estate"]
