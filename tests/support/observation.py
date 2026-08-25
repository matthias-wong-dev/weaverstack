"""One remote state transition, one evidence payload.

A statement submitted to a Fabric session costs seconds, so a test asking six
questions of the estate pays six times over, and, worse, gets six answers about
six different instants of a mutable remote estate. A claim like "after prune,
the declared objects are still there and the orphans are gone" is then not one
claim about one moment; it is several claims about several, and a later one can
be true of state an earlier one never saw.

So the shape here is: gather every question about a moment into one body, submit
it once, bring back one payload, and assert against it here. Assertion messages
stay readable, the estate is interrogated once, and the evidence can be kept on
the step it belongs to rather than re-read later.

The bodies are text rather than code called directly, because they have to run
where the estate is: inside a Fabric session, submitted over Livy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class Observation:
    """What one round trip saw, the evidence a transition is asserted against.

    Held rather than re-queried. A journey step keeps its own observation, so a
    later transition cannot repair what an earlier one broke and have the earlier
    assertion pass on state that no longer exists.
    """

    #: Evidence name -> the rows that statement returned, as plain dicts.
    rows: Mapping[str, list]
    #: Evidence name -> whether that schema exists.
    schemas: Mapping[str, bool] = field(default_factory=dict)
    #: Evidence name -> whether that qualified table exists. Asked of the
    #: catalogue rather than by querying it, because "absent" is a legitimate
    #: answer here and a SELECT against a missing table raises instead.
    tables: Mapping[str, bool] = field(default_factory=dict)

    def __getitem__(self, name: str) -> list:
        try:
            return self.rows[name]
        except KeyError:
            raise AssertionError(
                f"no evidence named {name!r} was observed; this payload carries "
                f"{sorted(self.rows)}"
            ) from None

    def scalar(self, name: str):
        """The single value of a single-row, single-column result."""

        rows = self[name]
        assert len(rows) == 1, f"{name!r} returned {len(rows)} rows, expected one"
        return next(iter(rows[0].values()))

    def values(self, name: str, column: str) -> set:
        """One column of names, lowercased.

        Case is the workspace's to choose: Fabric lowercases a managed table's
        directory exactly as a case-folding metastore does, so an exact-case
        comparison would assert something neither environment promises.

        Empty and null entries are dropped, `DESCRIBE` pads its output with
        blank rows to separate sections, and those are not object names.
        """

        return {str(row[column]).lower() for row in self[name] if row[column]}

    def schema(self, name: str) -> bool:
        return self._flag(self.schemas, name, "schema")

    def table(self, name: str) -> bool:
        return self._flag(self.tables, name, "table")

    @staticmethod
    def _flag(evidence: Mapping[str, bool], name: str, kind: str) -> bool:
        try:
            return evidence[name]
        except KeyError:
            raise AssertionError(
                f"no {kind} evidence named {name!r} was observed; this payload "
                f"carries {sorted(evidence)}"
            ) from None


def observe_body(
    queries: Mapping[str, str],
    schemas: Mapping[str, str] = (),
    tables: Mapping[str, str] = (),
) -> str:
    """The one body that collects every piece of evidence and emits it together.

     Every name reaching this is already qualified by the caller, because only the
     caller knows which of an estate's Lakehouses each piece of evidence is about
    , and spanning several of them in one payload is exactly the point.
    """

    return (
        "_rows = {}\n"
        f"for _name, _sql in {dict(queries)!r}.items():\n"
        "    _rows[_name] = [_r.asDict() for _r in spark.sql(_sql).collect()]\n"
        "_schemas = {}\n"
        f"for _name, _schema in {dict(schemas)!r}.items():\n"
        "    _schemas[_name] = bool(spark.catalog.databaseExists(_schema))\n"
        "_tables = {}\n"
        f"for _name, _table in {dict(tables)!r}.items():\n"
        "    _tables[_name] = bool(spark.catalog.tableExists(_table))\n"
        "emit({'rows': _rows, 'schemas': _schemas, 'tables': _tables})\n"
    )


def observation_from(payload) -> Observation:
    """One emitted payload, checked and turned into evidence."""

    assert payload is not None, "the observation body emitted no payload"
    return Observation(
        rows=payload["rows"], schemas=payload["schemas"], tables=payload["tables"]
    )


def observe_in_session(
    session, *, queries=None, schemas=None, tables=None
) -> Observation:
    """One Livy submission, one evidence payload: for a raw session.

    ``BuildEnv.observe`` is the same thing for a fixture that has a whole build
    environment and can resolve object tokens for itself. This serves the estates
    that hold only a session and a resolver, so those tests can stop spending a
    round trip per question.
    """

    return observation_from(
        session.run(observe_body(queries or {}, schemas or {}, tables or {})).payload
    )
