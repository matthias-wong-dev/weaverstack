"""A Spark session of its own for each load that runs beside others.

Spark is doubled as sessions holding runtime settings. Reading every setting
crosses py4j once per setting in Fabric, so the double counts the reads.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

from weaver.run.dispatch import isolated_spark
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


class Spark:
    def __init__(self, settings=DEFAULTS):
        self.reads = 0
        self.conf = Conf(self, settings)
        self.sessions: list[Spark] = []

    def newSession(self):
        session = Spark()
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
