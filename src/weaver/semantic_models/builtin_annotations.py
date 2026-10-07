"""Weaver's own semantic model annotations."""

from __future__ import annotations

import re
from fnmatch import fnmatchcase

from ..errors import ConfigError
from .annotation import Annotation, _pointer
from .extension_expectations import _ENDPOINT
from .references import source_identity
from .tmdl import object_name, quote_name


class Weaver__Source(Annotation):
    """Generate a table's source from a managed relation.

    The value is ``Warehouse/<item>/<schema>.<object>`` or
    ``Lakehouse/<item>/Tables/<schema>.<object>``. Build binds the table to that
    relation's endpoint and infers columns and descriptions from the catalogue.
    """

    scopes = frozenset({"table"})

    def apply(self, target):
        reference = self.value
        if reference.startswith("Lakehouse/") and (
            len(reference.split("/")) != 4 or reference.split("/")[2] != "Tables"
        ):
            self.error("use Lakehouse/<item>/Tables/<schema>.<object>")
        try:
            source_identity(reference)
        except ConfigError as exc:
            self.error(str(exc))
        compilation = self._compilation
        pointer = _pointer((("table", target.name),))
        origin = compilation.provenance.get(
            pointer + "/annotations/Weaver.Source/value", {}
        ).get("source", self._location.split(":", 1)[0])
        compilation.provenance[pointer + "/source"] = {
            "source": origin,
            "reason": "Weaver.Source",
            "reference": reference,
        }
        compilation.source_references[target.name] = reference


class Weaver__AutoHideColumns(Annotation):
    """Hide columns whose names match newline-separated glob patterns."""

    scopes = frozenset({"model", "table"})

    def apply(self, target):
        patterns = self.lines()
        if not patterns or any(not pattern for pattern in patterns):
            self.error("requires at least one glob pattern")
        tables = target.tables if target.parent is None else [target]
        for table in tables:
            for column in table.columns:
                if any(fnmatchcase(column.name, p) for p in patterns):
                    column.isHidden = True


class Weaver__AutoHideForeignKeys(Annotation):
    """When true, hide each relationship's many-side column."""

    scopes = frozenset({"model"})

    def apply(self, target):
        if not self.boolean():
            return
        for relationship in target.relationships:
            for side, default in (("from", "many"), ("to", "one")):
                if (relationship[side + "Cardinality"] or default) != "many":
                    continue
                endpoint = _ENDPOINT.fullmatch(str(relationship[side + "Column"] or ""))
                if endpoint is None:
                    self.error(
                        f"relationship {relationship.name!r} cannot address its "
                        "many-side column"
                    )
                table, column = object_name(endpoint[1]), object_name(endpoint[2])
                if table in target.tables and column in target.tables[table].columns:
                    target.tables[table].columns[column].isHidden = True


#: Native measure metadata under readable column names. DataType and
#: FormatString are omitted: a calculated table reads both as blank, and Fabric
#: fails to save a projection of DataType.
MEASURE_TABLE_COLUMNS = (
    ("Measure name", "Name"),
    ("Expression", "Expression"),
    ("Format string definition", "FormatStringDefinition"),
    ("Description", "Description"),
    ("Display folder", "DisplayFolder"),
    ("Table", "Table"),
    ("Data category", "DataCategory"),
)
MEASURE_TABLE_SOURCE = (
    "SELECTCOLUMNS(\n    INFO.VIEW.MEASURES(),\n"
    + ",\n".join(f'    "{name}", [{native}]' for name, native in MEASURE_TABLE_COLUMNS)
    + "\n)"
)
MEASURE_NAME_COLUMN = MEASURE_TABLE_COLUMNS[0][0]


class Weaver__MeasureTable(Annotation):
    """When true, generate the table from INFO.VIEW.MEASURES().

    Its columns are :data:`MEASURE_TABLE_COLUMNS`.
    """

    scopes = frozenset({"table"})

    def apply(self, target):
        if not self.boolean():
            self.error("requires true")
        partitions = list(target.partitions)
        if partitions and (
            len(partitions) != 1
            or partitions[0].name != target.name
            or partitions[0].sourceType != "calculated"
            or partitions[0].source != MEASURE_TABLE_SOURCE
        ):
            self.error("cannot replace an authored partition; use a bare table")
        partition = (
            partitions[0]
            if partitions
            else target.partitions.add(target.name, "calculated")
        )
        partition.sourceType = "calculated"
        partition.mode = "import"
        partition.set_expression("source", MEASURE_TABLE_SOURCE)


def _dax_string(value):
    return '"' + value.replace('"', '""') + '"'


class Weaver__Switch(Annotation):
    """Generate a measure that switches between the referenced measures.

    The value lists one measure per line, as ``Table[Measure]`` or a unique
    ``Measure``. The selector is the one table annotated Weaver.MeasureTable.
    """

    scopes = frozenset({"measure"})

    def apply(self, target):
        model = target.parent.parent
        measures = {}
        selectors = []
        for table in model.tables:
            for measure in table.measures:
                measures[(table.name.casefold(), measure.name.casefold())] = (
                    table.name,
                    measure,
                )
            if "Weaver.MeasureTable" in [a.name for a in table.annotations]:
                selectors.append(table.name)
        if len(selectors) != 1:
            self.error("requires one Weaver.MeasureTable selector")
        references = [line.strip() for line in self._text.splitlines() if line.strip()]
        if not references:
            self.error("requires at least one measure reference")
        values, formats = [], []
        labels = set()
        for reference in references:
            match = re.fullmatch(r"(.+)\[((?:[^\]]|\]\])+)\]", reference)
            if match:
                key = (
                    object_name(match[1].strip()).casefold(),
                    match[2].replace("]]", "]").casefold(),
                )
                matches = [measures[key]] if key in measures else []
            else:
                name = reference
                if name.startswith("[") and name.endswith("]"):
                    name = name[1:-1].replace("]]", "]")
                matches = [v for k, v in measures.items() if k[1] == name.casefold()]
            if len(matches) != 1:
                self.error(
                    f"{'ambiguous' if matches else 'missing'} measure {reference!r}"
                )
            table, measure = matches[0]
            name = measure.name
            if name.casefold() in labels:
                self.error(f"ambiguous selector label {name!r}")
            labels.add(name.casefold())
            if "Weaver.Switch" in [a.name for a in measure.annotations]:
                self.error(
                    f"switch measure reference {reference!r} is recursive or unsupported"
                )
            values.append(
                f"    {_dax_string(name)}, {quote_name(table)}[{name.replace(']', ']]')}]"
            )
            dynamic = measure.formatStringDefinition
            if dynamic:
                code = re.sub(
                    r'//[^\n]*|--[^\n]*|/\*.*?\*/|"(?:[^"]|"")*"',
                    "",
                    dynamic,
                    flags=re.S,
                )
                if re.search(
                    r"\b(?:SELECTEDMEASURE(?:NAME|FORMATSTRING)?|ISSELECTEDMEASURE)\s*\(",
                    code,
                    re.I,
                ):
                    self.error(
                        f"dynamic format for {reference!r} depends on measure context"
                    )
            formats.append(
                f"    {_dax_string(name)}, "
                + (dynamic or _dax_string(measure.formatString or ""))
            )
        selector = (
            f"SWITCH(\n    SELECTEDVALUE({quote_name(selectors[0])}"
            f"[{MEASURE_NAME_COLUMN}]),\n"
        )
        target.expression = selector + ",\n".join(values) + "\n)"
        target.set_expression(
            "formatStringDefinition", selector + ",\n".join(formats) + "\n)"
        )


class Weaver__Exclude(Annotation):
    """When true, remove the annotated table or column from the deployed model."""

    scopes = frozenset({"table", "column"})

    def apply(self, target):
        if self.boolean():
            target.remove()


BUILTIN_ANNOTATIONS = (
    Weaver__Source,
    Weaver__AutoHideColumns,
    Weaver__AutoHideForeignKeys,
    Weaver__MeasureTable,
    Weaver__Switch,
    Weaver__Exclude,
)
