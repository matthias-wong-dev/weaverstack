"""Exact-case Spark analysis scoped to one executor call."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

_CASE_SENSITIVE = "spark.sql.caseSensitive"


@contextmanager
def exact_identifier_case(spark, *, enabled: bool) -> Iterator[None]:
    """Temporarily make both analysis and DDL honour Weaver identifier case."""

    if not enabled:
        yield
        return
    previous = spark.conf.get(_CASE_SENSITIVE)
    if str(previous).lower() == "true":
        yield
        return
    spark.conf.set(_CASE_SENSITIVE, "true")
    try:
        yield
    finally:
        spark.conf.set(_CASE_SENSITIVE, previous)
