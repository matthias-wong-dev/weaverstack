"""A Spark session for code that does not use one.

Weaver objects take a Spark session because authored code runs through it, and a
great deal of what an object is, its identity, its resolved destination, its
catalogue anchor, whether a load has anything to do, is settled before any
engine is asked anything.

:class:`MockSpark` stands there for those paths. It emulates nothing: every
attribute access fails, naming what was asked for. That is what makes it useful
rather than a liability, a test that constructs an object with one and then
loads it successfully has proved no Spark call happened, instead of asserting
against a stand-in that might have answered differently from Spark.

Where a test needs Spark to answer something, it needs a real session
(``tests/fabric``) or a purpose-built fake in the module that needs it. Pass a
resolved Lakehouse, :func:`support.workspaces.mounted_lakehouse`, so
construction never has to infer one from the session.
"""

from __future__ import annotations


class MockSpark:
    """A session that is never used, and fails loudly if it is.

    ``label`` appears in the failure, so a test running several objects can tell
    which one reached for an engine.
    """

    def __init__(self, label: str = "MockSpark") -> None:
        self._label = label

    def __getattr__(self, name: str):
        raise AssertionError(
            f"{self._label} was asked for {name!r}: this path is meant to run "
            "without Spark. Use a real session, or a fake that answers the call "
            "the code under test actually makes."
        )

    def __repr__(self) -> str:
        return f"<{self._label}: unused>"


__all__ = ["MockSpark"]
