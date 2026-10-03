"""Concurrent Spark statements share one session-global identifier-case setting."""

from __future__ import annotations

import threading
import time

from support.weaver_test import weaver_test

from weaver.build_bundle.executors.spark_case import exact_identifier_case

SETTING = "spark.sql.caseSensitive"


class _Spark:
    def __init__(self, value="false"):
        self.values = {SETTING: value}
        self.conf = self
        self.sets = []

    def get(self, name):
        return self.values[name]

    def set(self, name, value):
        self.sets.append(value)
        self.values[name] = value


@weaver_test()
def test_overlapping_exact_scopes_set_once_and_restore_after_the_last():
    spark = _Spark()
    inside = threading.Barrier(2)
    seen = []

    def exact():
        with exact_identifier_case(spark, enabled=True):
            inside.wait()
            seen.append(spark.values[SETTING])
            inside.wait()

    threads = [threading.Thread(target=exact) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)

    assert seen == ["true", "true"]
    assert spark.sets == ["true", "false"]
    assert spark.values[SETTING] == "false"


@weaver_test()
def test_a_default_case_scope_waits_for_exact_scopes_to_leave():
    spark = _Spark()
    entered = threading.Event()
    release = threading.Event()
    observed = []

    def exact():
        with exact_identifier_case(spark, enabled=True):
            entered.set()
            release.wait(5)

    def default():
        with exact_identifier_case(spark, enabled=False):
            observed.append(spark.values[SETTING])

    first = threading.Thread(target=exact)
    first.start()
    entered.wait(5)
    second = threading.Thread(target=default)
    second.start()
    time.sleep(0.05)
    assert observed == []
    release.set()
    first.join(5)
    second.join(5)

    assert observed == ["false"]


@weaver_test()
def test_an_already_exact_session_is_left_alone():
    spark = _Spark("true")

    with exact_identifier_case(spark, enabled=True):
        pass

    assert spark.sets == []
