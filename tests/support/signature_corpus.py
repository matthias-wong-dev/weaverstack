"""Rows built to break a row signature, and the changes made to them.

A row signature is a digest of the row's comparison columns written as text. A
digest does not collide by accident at this scale, so a signature that misses a
change does so because two different rows were written as the same text. Every
case here aims at one way that can happen:

.. code-block:: text

    null, empty, whitespace       the null marker and an empty or blank value
    framing                       ("ab", "c") against ("a", "bc"), separators,
                                  the marker and length digits inside values
    position                      the same values in different columns
    text                          case, composed and decomposed accents, emoji,
                                  non-Latin scripts, control characters, quotes,
                                  100,000 characters differing in one place
    numbers                       integer limits, decimal scale and limits,
                                  binary floating point, number against text
    time                          microseconds, the ends of each range
    staged type                   a float staged for a decimal column
    nested                        arrays, maps and structs (Lakehouse)

Each engine signs the cases whose columns it has. Two cases whose comparison
values are equal must sign equally and any two others must not, so the expected
partition is computed from the values themselves.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import date, datetime, time, timezone
from decimal import Decimal

LAKEHOUSE = "Lakehouse"
WAREHOUSE = "Warehouse"

KEY = "CaseId"


@dataclass(frozen=True)
class Column:
    """One comparison column.

    ``spark`` and ``tsql`` are the declared target types, ``None`` where the
    engine has no such column. ``staged_spark`` and ``staged_tsql`` are the
    source column's type when the value is staged as something else.
    """

    name: str
    spark: str | None
    tsql: str | None
    staged_spark: str | None = None
    staged_tsql: str | None = None

    def target(self, engine: str) -> str | None:
        return self.spark if engine == LAKEHOUSE else self.tsql

    def source(self, engine: str) -> str | None:
        if engine == LAKEHOUSE:
            return self.staged_spark or self.spark
        return self.staged_tsql or self.tsql


COLUMNS = (
    Column("Text1", "string", "varchar(8000)"),
    Column("Text2", "string", "varchar(8000)"),
    Column("Note", "string", "varchar(max)"),
    Column("Whole", "bigint", "bigint"),
    Column("Amount", "decimal(38,10)", "decimal(38,10)"),
    Column("Ratio", "double", "float"),
    Column("Flag", "boolean", "bit"),
    Column("Day", "date", "date"),
    Column("Instant", "timestamp", "datetime2(6)"),
    Column("Bytes", "binary", "varbinary(64)"),
    # Staged as binary floating point, stored as a decimal.
    Column(
        "Measured",
        "decimal(18,4)",
        "decimal(18,4)",
        staged_spark="double",
        staged_tsql="float",
    ),
    Column("Clock", None, "time(6)"),
    Column("Local", "timestamp_ntz", None),
    Column("Tags", "array<string>", None),
    Column("Attrs", "map<string,string>", None),
    Column("Point", "struct<a:string,b:string>", None),
)

BY_NAME = {column.name: column for column in COLUMNS}


def columns(engine: str) -> tuple[Column, ...]:
    return tuple(c for c in COLUMNS if c.target(engine) is not None)


@dataclass(frozen=True)
class Case:
    id: str
    values: dict = field(default_factory=dict)

    def applies_to(self, engine: str) -> bool:
        return all(BY_NAME[name].target(engine) for name in self.values)

    def row(self, engine: str) -> tuple:
        return tuple(self.values.get(c.name) for c in columns(engine))

    def changed(self, **values) -> "Case":
        return Case(self.id, {**self.values, **values})


LONG = "".join(chr(ord("a") + (i % 26)) for i in range(100_000))
MIDDLE = len(LONG) // 2


def _instant(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


CASES = (
    Case("all-null"),
    # Null, empty and blank, in each position.
    Case("text1-empty", {"Text1": ""}),
    Case("text1-space", {"Text1": " "}),
    Case("text1-spaces", {"Text1": "  "}),
    Case("text2-empty", {"Text2": ""}),
    Case("text2-space", {"Text2": " "}),
    Case("lead-space", {"Text1": " a"}),
    Case("trail-space", {"Text1": "a "}),
    Case("plain-a", {"Text1": "a"}),
    # Framing: concatenations that read the same without lengths.
    Case("split-ab-c", {"Text1": "ab", "Text2": "c"}),
    Case("split-a-bc", {"Text1": "a", "Text2": "bc"}),
    Case("split-abc-null", {"Text1": "abc"}),
    Case("split-null-abc", {"Text2": "abc"}),
    Case("split-abc-empty", {"Text1": "abc", "Text2": ""}),
    Case("split-empty-abc", {"Text1": "", "Text2": "abc"}),
    Case("digits-12-3", {"Text1": "12", "Text2": "3"}),
    Case("digits-1-23", {"Text1": "1", "Text2": "23"}),
    Case("marker", {"Text1": "~"}),
    Case("marker-pair", {"Text1": "~", "Text2": "~"}),
    Case("colon", {"Text1": ":"}),
    Case("framed-pair", {"Text1": "1:a", "Text2": "1:b"}),
    Case("framed-one", {"Text1": "1:a1:b"}),
    Case("framed-marker", {"Text1": "a", "Text2": "1:~"}),
    Case("framed-marker-joined", {"Text1": "a1:~"}),
    Case("separators", {"Text1": "a|b,c;d\te\nf"}),
    # Position: the same values in different columns.
    Case("swap-xy", {"Text1": "x", "Text2": "y"}),
    Case("swap-yx", {"Text1": "y", "Text2": "x"}),
    Case("mixed-nulls-1", {"Text2": "", "Whole": 0, "Flag": False, "Bytes": b""}),
    Case("mixed-nulls-2", {"Text1": "", "Amount": Decimal("0"), "Flag": False}),
    Case("mixed-nulls-3", {"Text1": "", "Text2": None, "Whole": 0, "Bytes": b""}),
    # Text.
    Case("lower", {"Text1": "abcd"}),
    Case("upper", {"Text1": "ABCD"}),
    Case("composed", {"Text1": "é"}),
    Case("decomposed", {"Text1": "é"}),
    Case("emoji", {"Text1": "\U0001f600"}),
    Case("emoji-next", {"Text1": "\U0001f601"}),
    Case("family", {"Text1": "\U0001f468‍\U0001f469‍\U0001f467"}),
    Case("cjk", {"Text1": "数据"}),
    Case("arabic", {"Text1": "بيانات"}),
    Case("accented", {"Text1": "Ünïcødé ✓"}),
    Case("newline", {"Text1": "a\nb"}),
    Case("crlf", {"Text1": "a\r\nb"}),
    Case("quote", {"Text1": 'O\'Brien "q"'}),
    Case("backslash", {"Text1": "a\\b"}),
    Case("long", {"Note": LONG}),
    Case("long-end", {"Note": LONG[:-1] + "!"}),
    Case("long-middle", {"Note": LONG[:MIDDLE] + "!" + LONG[MIDDLE + 1 :]}),
    Case("long-longer", {"Note": LONG + "a"}),
    # Integers, and number against text.
    Case("whole-zero", {"Whole": 0}),
    Case("whole-one", {"Whole": 1}),
    Case("whole-negative", {"Whole": -1}),
    Case("whole-max", {"Whole": 2**63 - 1}),
    Case("whole-min", {"Whole": -(2**63)}),
    Case("text-one", {"Text1": "1"}),
    Case("text-decimal", {"Text1": "1.5"}),
    # Decimals. 1.5 and 1.50 are one value in a decimal(38,10) column.
    Case("amount-zero", {"Amount": Decimal("0")}),
    Case("amount-1.5", {"Amount": Decimal("1.5")}),
    Case("amount-1.50", {"Amount": Decimal("1.50")}),
    Case("amount-negative", {"Amount": Decimal("-1.5")}),
    Case("amount-tiny", {"Amount": Decimal("0.0000000001")}),
    Case("amount-max", {"Amount": Decimal("9999999999999999999999999999.9999999999")}),
    Case("amount-min", {"Amount": Decimal("-9999999999999999999999999999.9999999999")}),
    # Binary floating point.
    Case("ratio-0.3", {"Ratio": 0.3}),
    Case("ratio-0.1+0.2", {"Ratio": 0.1 + 0.2}),
    Case("ratio-one", {"Ratio": 1.0}),
    Case("ratio-largest", {"Ratio": 1.7976931348623157e308}),
    Case("ratio-smallest", {"Ratio": 2.2250738585072014e-308}),
    Case("ratio-negative", {"Ratio": -2.5}),
    Case("ratio-digits", {"Ratio": 1234567.1234567}),
    Case("ratio-digits-next", {"Ratio": 1234567.1234568}),
    # Staged as float into decimal(18,4).
    Case("measured", {"Measured": 1234567.1234}),
    Case("measured-next", {"Measured": 1234567.1235}),
    Case("measured-small", {"Measured": 0.0001}),
    Case("measured-small-next", {"Measured": 0.0002}),
    # Booleans.
    Case("flag-true", {"Flag": True}),
    Case("flag-false", {"Flag": False}),
    # Dates and times.
    Case("day-leap", {"Day": date(2024, 2, 29)}),
    Case("day-first", {"Day": date(1, 1, 1)}),
    Case("day-last", {"Day": date(9999, 12, 31)}),
    Case("instant-whole", {"Instant": _instant("2024-01-01T00:00:00")}),
    Case("instant-1us", {"Instant": _instant("2024-01-01T00:00:00.000001")}),
    Case("instant-2us", {"Instant": _instant("2024-01-01T00:00:00.000002")}),
    Case("instant-old", {"Instant": _instant("1900-01-01T00:00:00")}),
    Case("instant-last", {"Instant": _instant("9999-12-31T23:59:59.999999")}),
    Case("clock-midnight", {"Clock": time(0, 0)}),
    Case("clock-1us", {"Clock": time(12, 0, 0, 1)}),
    Case("clock-2us", {"Clock": time(12, 0, 0, 2)}),
    Case("clock-last", {"Clock": time(23, 59, 59, 999999)}),
    Case("local-1us", {"Local": datetime(2024, 1, 1, 0, 0, 0, 1)}),
    Case("local-2us", {"Local": datetime(2024, 1, 1, 0, 0, 0, 2)}),
    # Bytes.
    Case("bytes-empty", {"Bytes": b""}),
    Case("bytes-zero", {"Bytes": b"\x00"}),
    Case("bytes-zeros", {"Bytes": b"\x00\x00"}),
    Case("bytes-marker", {"Bytes": b"~"}),
    Case("bytes-high", {"Bytes": b"\xff"}),
    # Nested values, whose elements Spark's own text neither quotes nor escapes.
    Case("tags-joined", {"Tags": ["a, b"]}),
    Case("tags-split", {"Tags": ["a", "b"]}),
    Case("tags-null-element", {"Tags": [None]}),
    Case("tags-null-text", {"Tags": ["null"]}),
    Case("tags-none", {"Tags": []}),
    Case("tags-empty-element", {"Tags": [""]}),
    Case("attrs-joined", {"Attrs": {"k": "v, x -> y"}}),
    Case("attrs-split", {"Attrs": {"k": "v", "x": "y"}}),
    Case("attrs-none", {"Attrs": {}}),
    Case("attrs-null-value", {"Attrs": {"k": None}}),
    Case("point-joined", {"Point": ("1, 2", None)}),
    Case("point-split", {"Point": ("1", "2, null")}),
    Case("point-nulls", {"Point": (None, None)}),
    # Several unusual values at once, and twins that must sign equally.
    Case(
        "everything",
        {
            "Text1": "~:1",
            "Text2": " ",
            "Note": "",
            "Whole": -(2**63),
            "Amount": Decimal("-0.0000000001"),
            "Ratio": -1.7976931348623157e308,
            "Flag": False,
            "Day": date(1, 1, 1),
            "Instant": _instant("1900-01-01T00:00:00.000001"),
            "Bytes": b"\x00",
            "Measured": 0.0001,
        },
    ),
    Case("twin", {"Text1": "twin", "Amount": Decimal("2.5"), "Flag": True}),
    Case("twin-again", {"Text1": "twin", "Amount": Decimal("2.5"), "Flag": True}),
)


#: One change to one case's tracked values. Each would be missed by a payload
#: that wrote the old and new rows as the same text.
CHANGES = {
    "all-null": {"Text1": ""},
    "text1-empty": {"Text1": " "},
    "text1-space": {"Text1": None},
    "lead-space": {"Text1": "a "},
    "split-ab-c": {"Text1": "a", "Text2": "bc"},
    "split-abc-null": {"Text1": None, "Text2": "abc"},
    "digits-12-3": {"Text1": "1", "Text2": "23"},
    "marker": {"Text1": None},
    "framed-pair": {"Text1": "1:a1:b", "Text2": None},
    "swap-xy": {"Text1": "y", "Text2": "x"},
    "lower": {"Text1": "ABCD"},
    "composed": {"Text1": "é"},
    "emoji": {"Text1": "\U0001f601"},
    "crlf": {"Text1": "a\nb"},
    "long": {"Note": LONG[:MIDDLE] + "!" + LONG[MIDDLE + 1 :]},
    "whole-max": {"Whole": 2**63 - 2},
    "amount-tiny": {"Amount": Decimal("0.0000000002")},
    "ratio-0.3": {"Ratio": 0.1 + 0.2},
    "ratio-digits": {"Ratio": 1234567.1234568},
    "measured": {"Measured": 1234567.1235},
    "flag-false": {"Flag": None},
    "day-leap": {"Day": date(2024, 3, 1)},
    "instant-1us": {"Instant": _instant("2024-01-01T00:00:00.000002")},
    "clock-1us": {"Clock": time(12, 0, 0, 2)},
    "local-1us": {"Local": datetime(2024, 1, 1, 0, 0, 0, 2)},
    "bytes-zero": {"Bytes": b"\x00\x00"},
    "tags-split": {"Tags": ["a, b"]},
    "tags-null-text": {"Tags": [None]},
    "attrs-split": {"Attrs": {"k": "v, x -> y"}},
    "point-split": {"Point": ("1, 2", None)},
    "mixed-nulls-1": {"Text1": "", "Text2": None},
}

#: Rewritten with a value equal to the one held, so not a change.
REWRITES = {"amount-1.5": {"Amount": Decimal("1.50")}}

INSERTS = (
    Case("inserted-null"),
    Case("inserted-framed", {"Text1": "a", "Text2": "bc", "Whole": 1}),
)

DELETES = ("twin-again", "whole-min")


def cases(engine: str) -> tuple[Case, ...]:
    return tuple(case for case in CASES if case.applies_to(engine))


def changed_cases(engine: str) -> tuple[Case, ...]:
    """The corpus after the changes, rewrites, inserts and deletes."""

    edits = {**CHANGES, **REWRITES}
    kept = tuple(
        case.changed(**edits[case.id]) if case.id in edits else case
        for case in cases(engine)
        if case.id not in DELETES
    )
    return kept + tuple(case for case in INSERTS if case.applies_to(engine))


def changed_ids(engine: str) -> set[str]:
    present = {case.id for case in cases(engine)}
    return {
        case_id
        for case_id, values in CHANGES.items()
        if case_id in present and all(BY_NAME[n].target(engine) for n in values)
    }


def same_values(left: Case, right: Case, engine: str) -> bool:
    return left.row(engine) == right.row(engine)


def expected_partition(engine: str, corpus=None) -> set[frozenset[str]]:
    """Case ids grouped by equal comparison values."""

    groups: list[list[Case]] = []
    for case in corpus if corpus is not None else cases(engine):
        for group in groups:
            if same_values(group[0], case, engine):
                group.append(case)
                break
        else:
            groups.append([case])
    return {frozenset(case.id for case in group) for group in groups}


def partition(signatures: dict[str, object]) -> set[frozenset[str]]:
    """Case ids grouped by equal signature."""

    groups: dict[object, set[str]] = {}
    for case_id, signature in signatures.items():
        groups.setdefault(signature, set()).add(case_id)
    return {frozenset(group) for group in groups.values()}


def collisions(signatures: dict[str, object], engine: str, corpus=None) -> list:
    """Groups the signatures put together that the values keep apart."""

    expected = expected_partition(engine, corpus)
    return sorted(
        sorted(group)
        for group in partition(signatures)
        if group not in expected and len(group) > 1
    )


# --- Spark literals ----------------------------------------------------------


def _spark_text(value: str) -> str:
    # Base64 carries any text through the statement unchanged.
    encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
    return f"CAST(unbase64('{encoded}') AS STRING)"


def spark_literal(value, kind: str) -> str:
    if value is None:
        return f"CAST(NULL AS {kind})"
    if kind == "string":
        return _spark_text(value)
    if kind in ("bigint", "boolean"):
        return f"CAST('{str(value).lower()}' AS {kind})"
    if kind.startswith("decimal"):
        return f"CAST('{value:f}' AS {kind})"
    if kind == "double":
        return f"CAST('{value!r}' AS DOUBLE)"
    if kind == "date":
        return f"DATE'{value.isoformat()}'"
    if kind == "timestamp":
        micros = (value - datetime(1970, 1, 1, tzinfo=timezone.utc)) // _MICROSECOND
        return f"timestamp_micros({micros})"
    if kind == "timestamp_ntz":
        return f"CAST('{value.isoformat(sep=' ')}' AS TIMESTAMP_NTZ)"
    if kind == "binary":
        return f"unhex('{value.hex()}')"
    if kind == "array<string>":
        elements = ", ".join(spark_literal(v, "string") for v in value)
        return f"CAST(array({elements}) AS ARRAY<STRING>)"
    if kind == "map<string,string>":
        pairs = ", ".join(
            f"{spark_literal(k, 'string')}, {spark_literal(v, 'string')}"
            for k, v in value.items()
        )
        return f"CAST(map({pairs}) AS MAP<STRING,STRING>)"
    if kind == "struct<a:string,b:string>":
        a, b = (spark_literal(v, "string") for v in value)
        return f"named_struct('a', {a}, 'b', {b})"
    raise ValueError(f"no Spark literal for {kind}")


_MICROSECOND = datetime(1970, 1, 1, 0, 0, 0, 1) - datetime(1970, 1, 1)


def spark_rows(corpus, engine: str = LAKEHOUSE) -> str:
    """The cases as a Spark SQL relation of source-typed columns."""

    selects = []
    for case in corpus:
        values = [f"{_spark_text(case.id)} AS `{KEY}`"]
        for column, value in zip(columns(engine), case.row(engine), strict=True):
            values.append(
                f"{spark_literal(value, column.source(engine))} AS `{column.name}`"
            )
        selects.append("SELECT " + ", ".join(values))
    return "\nUNION ALL\n".join(selects)


# --- T-SQL literals ----------------------------------------------------------


def tsql_literal(value, kind: str) -> str:
    if value is None:
        return "null"
    if kind.startswith("varchar"):
        return "N'" + value.replace("'", "''") + "'"
    if kind == "bit":
        return "1" if value else "0"
    if kind == "varbinary(64)":
        return "0x" + value.hex()
    if kind == "float":
        return f"cast('{value!r}' as float)"
    if kind == "datetime2(6)":
        return f"cast('{value.replace(tzinfo=None).isoformat()}' as datetime2(6))"
    if kind in ("date", "time(6)"):
        return f"cast('{value.isoformat()}' as {kind})"
    if isinstance(value, Decimal):
        value = f"{value:f}"
    return f"cast('{value}' as {kind})"


def tsql_rows(corpus) -> str:
    """The cases as a T-SQL VALUES list of source-typed columns."""

    rows = []
    for case in corpus:
        values = [tsql_literal(case.id, "varchar(64)")]
        for column, value in zip(columns(WAREHOUSE), case.row(WAREHOUSE), strict=True):
            values.append(tsql_literal(value, column.source(WAREHOUSE)))
        rows.append("(" + ", ".join(values) + ")")
    return ",\n".join(rows)


def tsql_source_columns() -> str:
    return ", ".join(
        [f"[{KEY}] varchar(64) not null"]
        + [f"[{c.name}] {c.source(WAREHOUSE)} null" for c in columns(WAREHOUSE)]
    )


def tsql_staged_select() -> str:
    return ", ".join([f"[{KEY}]"] + [f"[{c.name}]" for c in columns(WAREHOUSE)])


__all__ = [
    "CASES",
    "CHANGES",
    "COLUMNS",
    "DELETES",
    "INSERTS",
    "KEY",
    "LAKEHOUSE",
    "REWRITES",
    "WAREHOUSE",
    "Case",
    "changed_cases",
    "changed_ids",
    "collisions",
    "columns",
    "expected_partition",
    "partition",
    "spark_rows",
    "tsql_rows",
]
