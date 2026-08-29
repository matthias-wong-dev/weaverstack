"""A thin run: every real link in the chain, and nothing at the end.

.. code-block:: text

    Runner → dispatch → Session → real import → a trivial installed artefact

Everything in that chain is production code. The only thing not real is what the
primitive does, and that is the point: a thin run proves the wiring. That the
Runner reaches dispatch, that dispatch resolves the deployed artefact and reaches
it through the Session, and that whatever comes back is settled into the run's
own vocabulary. What a primitive would have done to data is a Primitive's claim,
and is proven against real engines elsewhere.

**No build, and no estate to speak of.** A run needs two things to reach a
primitive: a catalogue saying it is installed, and the artefact being where the
catalogue says. Both are arranged directly here. The Registry points at these
artefacts exactly as it points at production ones, which is what makes the
Runner unable to tell the difference, and it is why paying for a build to make
a primitive callable is unnecessary when the claim is about dispatch.

Each meets the contract a deployed load primitive meets, ``cls(spark,
lakehouse=...)`` and ``_load(fault_tolerant=...)``. The lower interface, because
that is the one a run calls: ``load()`` is the standalone wrapper that records,
and a run recording through a primitive would be a second writer of the same row.

Each artefact exists to produce one controlled physical outcome, chosen because
each is settled by a different rule:

.. code-block:: text

    Success      a result reporting success
    Rejects      rows written and rows refused, tolerated, or not
    Failure      a reported failure, nothing raised
    Raises       an exception the primitive never normalised
    Malformed    something that is not a result at all

    Agrees       a validation finding nothing
    Disagrees    a validation finding a discrepancy on both sides
    Unreadable   a validation that could not be evaluated at all

Loads need no Spark session at all, because a trivial load never touches one,
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
    item_bindings,
    item_id,
    lakehouse_table,
    lakehouse_test,
    single_document_repository,
)

from support.sessions import given_session
from support.workspaces import given_resolver, given_workspace
from weaver.etl import item_runtime_artefacts
from weaver.run import RunState
from weaver.store import FilesystemStore
from weaver.targets import ItemRef, PhysicalTargetRef

#: The schema these artefacts are declared in, so a thin node is recognisable in
#: a report as trivial rather than mistaken for an estate someone cared about.
SCHEMA = "Thin"

#: One trivial artefact per outcome. The class name is the module name, which is
#: the rule the deployed tree already follows, so these are found the way any
#: deployed module is found, not by a special case.
ARTEFACTS = {
    "Success": '''\
from weaver.runtime.load_result import LoadResult


class {name}:
    """Succeeds, having done nothing. The wiring is the whole claim."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def _load(self, fault_tolerant=False):
        return LoadResult(succeeded=True, rows_read=2, rows_inserted=2)
''',
    "Rejects": '''\
from weaver.errors import LoadError
from weaver.runtime.load_result import LoadResult


class {name}:
    """Refuses one row, and answers the way the real runtime answers.

    Both halves matter, and both are spelled as ``table_load`` spells them.
    A tolerated rejection returns a result marked failed that counted its
    rejects; an intolerant one raises, carrying the same counts. The two
    results are indistinguishable, the raising is the only difference, which
    is precisely why a fixture that invented a tidier spelling would prove
    the Runner settles something no primitive ever sends it.
    """

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def _load(self, fault_tolerant=False):
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

    def _load(self, fault_tolerant=False):
        return LoadResult.failure("the source system said no")
''',
    "Raises": '''\
class {name}:
    """Raises something the primitive never normalised."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def _load(self, fault_tolerant=False):
        raise RuntimeError("the source system was unreachable")
''',
    "Malformed": '''\
class {name}:
    """Returns something that cannot say whether it succeeded."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def _load(self, fault_tolerant=False):
        return {{"rows": 4}}
''',
}

OUTCOMES = tuple(ARTEFACTS)

#: A Test's deployed artefact is read rather than called, and what it returns is
#: a real Spark frame of sides, so these need a session where a load's do not.
#: Two literal rows are enough to settle each outcome: what a Test means is the
#: comparison's own claim, and what these settle is how the run reports it.
VALIDATIONS = {
    "Agrees": '''from pyspark.sql.types import StringType, StructField, StructType

SIDES = StructType([StructField("_weaver_side", StringType())])


class {name}:
    """Both sides agree: no rows, so nothing missing and nothing unexpected."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def read(self):
        return self.spark.createDataFrame([], SIDES)
''',
    "Disagrees": '''from pyspark.sql.types import StringType, StructField, StructType

SIDES = StructType([StructField("_weaver_side", StringType())])


class {name}:
    """One row expected and never seen, one seen and never expected."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def read(self):
        return self.spark.createDataFrame(
            [("expected",), ("actual",), ("actual",)], SIDES
        )
''',
    "Unreadable": '''class {name}:
    """Cannot be evaluated at all, which is not the same as finding nothing."""

    def __init__(self, spark, lakehouse=None):
        self.spark = spark

    def read(self):
        raise RuntimeError("the table this Test reads does not exist")
''',
}

JUDGEMENTS = tuple(VALIDATIONS)


@dataclass(frozen=True)
class ThinEstate:
    """What a thin run needs, and nothing a build would have brought with it."""

    session: Any
    workspace: Any
    state: Any
    #: What a run names: the logical item.
    item: Any
    #: Where it is installed, which the node ids below are spelled with.
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
        validation report is keyed by the logical id,
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
    judgements: tuple[str, ...] = (),
    lakehouse: str = "Thin_LH",
    session=None,
    resolver=None,
    store=None,
    workspace=None,
) -> ThinEstate:
    """A RunState and deployed artefacts for what was named, and a Session.

    The catalogue is composed by the same production constructors that describe
    a real estate, so what a thin run plans against is the shape a build
    publishes. Only the artefacts it points at are trivial.

    A thin run reaches a primitive and settles what comes back; it never touches
    data. What varies is where the artefacts are deployed and who dispatches
    to them, which is why the store, resolver and Session are injectable: given
    a real workspace's resolver and a OneLake store, the same builder deploys
    into Fabric and the same claims are made against the session that imports
    them there.
    """

    documents = {
        f"{SCHEMA}__{name}.py": lakehouse_table(f"{SCHEMA}.{name}") for name in outcomes
    }
    # Under ``tests/``, which is where a repository declares one: the folder is
    # what makes it a validation rather than another table.
    documents.update(
        {
            f"tests/{SCHEMA}__{name}.py": lakehouse_test(f"{SCHEMA}.{name}")
            for name in judgements
        }
    )
    repository = single_document_repository(
        root / "repository", schemas=(SCHEMA,), documents=documents
    )
    bindings = item_bindings(("Lakehouse/Sales", lakehouse))

    if workspace is None:
        workspace = given_workspace(catalogue="Warehouse/Weaver_LH")
    if resolver is None:
        resolver = given_resolver(
            workspace=workspace, lakehouses=("Weaver_LH", lakehouse), root=root
        )
    deployed = store if store is not None else FilesystemStore()

    # Written where the build would have written them, asked of the build's own
    # enumerator rather than assembled from a path this module believes in. A
    # load module and a Test module do not share a directory, and a fixture that
    # guessed would only be pinning the guess.
    files_root = resolver.files_root(ItemRef(lakehouse))
    sources = {f"{SCHEMA}__{name}": ARTEFACTS[name] for name in outcomes}
    sources.update({f"{SCHEMA}__{name}": VALIDATIONS[name] for name in judgements})
    for artefact in item_runtime_artefacts(repository, item=item_id("Lakehouse/Sales")):
        module = artefact.identity.object_id.object.removesuffix(".py")
        source = sources.get(module)
        if source is None:
            continue
        # Written through a Store rather than a Path, because the store is what
        # differs between a temporary directory and OneLake.
        deployed.write(
            files_root / artefact.target_path,
            source.format(name=module).encode("utf-8"),
        )

    target = PhysicalTargetRef("lakehouse", lakehouse)
    opened = (
        session
        if session is not None
        else given_session(
            workspace=workspace,
            store=deployed,
            resolver=resolver,
            # Without a Session of its own a thin run reaches its primitive in
            # this process, which is what made it thin.
            executes_here=True,
        )
    )
    return ThinEstate(
        session=opened,
        workspace=workspace,
        state=RunState(
            catalogue=installed_catalogue(repository, bindings, session=opened)
        ),
        item=item_id("Lakehouse/Sales"),
        target=target,
        root=root,
        nodes={
            **{name: f"load:{target}/{SCHEMA}.{name}" for name in outcomes},
            **{name: f"{target}/{SCHEMA}.{name}" for name in judgements},
        },
    )


__all__ = ["ARTEFACTS", "OUTCOMES", "SCHEMA", "ThinEstate", "thin_estate"]
