"""A Spark session of its own for each load that runs beside others.

Spark is doubled as sessions holding runtime settings. Reading every setting
crosses py4j once per setting in Fabric, so the double counts the reads.
"""

from __future__ import annotations

import threading

from support.weaver_test import weaver_test

from weaver.run.dispatch import isolated_spark, own_pool
from weaver.run.runtime_boundary import DirectRunScope

DEFAULTS = {"spark.sql.shuffle.partitions": "200", "spark.sql.caseSensitive": "false"}


class Conf:
    def __init__(self, spark, settings):
        self._spark = spark
        self.settings = dict(settings)

    @property
    def getAll(self):
        self._spark.reads += 1
        return dict(self.settings)

    def set(self, key, value):
        self.settings[key] = value


class Context:
    """The application, whose local properties belong to the calling thread."""

    def __init__(self):
        self.properties: dict = {}

    def getLocalProperty(self, key):  # noqa: N802 - Spark's name
        return self.properties.get((threading.get_ident(), key))

    def setLocalProperty(self, key, value):  # noqa: N802 - Spark's name
        self.properties[(threading.get_ident(), key)] = value


class Spark:
    def __init__(self, settings=DEFAULTS, context=None):
        self.reads = 0
        self.conf = Conf(self, settings)
        self.sessions: list[Spark] = []
        self.sparkContext = context or Context()

    def newSession(self):
        session = Spark(context=self.sparkContext)
        self.sessions.append(session)
        return session


class Session:
    def __init__(self, spark):
        self._spark = spark

    def spark(self, workspace=None):
        return self._spark


def _parent():
    return Spark(
        {
            **DEFAULTS,
            "spark.sql.shuffle.partitions": "8",
            "spark.fabric.pool": "starter",
            # Held by a load running on the parent at that moment.
            "spark.sql.caseSensitive": "true",
        }
    )


@weaver_test()
def test_a_session_of_its_own_starts_from_the_parents_settings():
    parent = _parent()

    session = isolated_spark(parent)

    assert session.conf.settings["spark.sql.shuffle.partitions"] == "8"
    assert session.conf.settings["spark.fabric.pool"] == "starter"


@weaver_test()
def test_a_setting_a_load_holds_for_one_statement_is_not_passed_on():
    session = isolated_spark(_parent())

    assert session.conf.settings["spark.sql.caseSensitive"] == "false"


@weaver_test()
def test_a_run_reads_the_parents_settings_once():
    parent = _parent()
    scope = DirectRunScope(runtime_scope=None, session=Session(parent))

    sessions = [scope._spark(isolated=True) for _ in range(5)]

    assert parent.reads == 1
    assert all(one.conf.settings["spark.fabric.pool"] == "starter" for one in sessions)
    assert scope._spark(isolated=False) is None


# --- each load's jobs in a pool of their own -------------------------------------


@weaver_test()
def test_a_load_runs_its_jobs_in_a_pool_of_its_own_and_gives_it_back():
    """Jobs naming no pool queue behind each other, first come first served."""

    spark = Spark()
    context = spark.sparkContext

    with own_pool(spark):
        inside = context.getLocalProperty("spark.scheduler.pool")

    assert inside == f"weaver-{threading.current_thread().name}"
    assert context.getLocalProperty("spark.scheduler.pool") is None


@weaver_test()
def test_a_spark_without_pools_runs_the_load_as_it_is():
    with own_pool(None):
        ran = True

    assert ran


@weaver_test()
def test_a_dispatched_load_is_in_its_threads_pool(monkeypatch):
    from weaver.run import dispatch

    parent = _parent()
    seen = []

    class Result:
        def as_row(self):
            return {}

    def python_primitive(*, spark, **kwargs):
        seen.append(spark.sparkContext.getLocalProperty("spark.scheduler.pool"))
        return Result()

    monkeypatch.setattr(dispatch, "python_primitive", python_primitive)
    scope = DirectRunScope(runtime_scope=None, session=Session(parent))

    class Node:
        node_id = "load:Lakehouse/Sales/Tables/Sales.Order"
        logical_id = type("Id", (), {"item": "Lakehouse/Sales"})()
        physical_target = "Lakehouse/Sales"
        primitive_object = type("Object", (), {"schema": "Sales", "object": "Order"})()

    scope.dispatch_python(
        Node, expected_class="Sales__Order", fault_tolerant=False, isolated=True
    )

    assert seen == [f"weaver-{threading.current_thread().name}"]
