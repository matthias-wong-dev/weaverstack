"""Semantic model projects the Fabric semantic tests build, parsed offline too."""

import shutil
from pathlib import Path

from support.semantic_models import policy_path
from weaver.catalogue.render import InstallationScope
from weaver.declaration.model import WeaverDocumentId, WeaverItemId

ITEM = WeaverItemId.parse("SemanticModel/RefreshAcceptance")
ROOT = WeaverDocumentId.model_root(ITEM)
SCOPE = InstallationScope(ITEM.item_type, ITEM.item_name)
PBIP = Path(__file__).parents[1] / "fixtures/semantic_model/Probe"

SOURCE_TEXT = """model Model
    annotation Weaver.AutoHideColumns = "Schema*"
    annotation Weaver.AutoHideForeignKeys = true

table Objects
    annotation Weaver.Source = Warehouse/_weaver/_.TableDictionary

    column 'Object type'
        annotation Weaver.Exclude = true

    measure Rows = COUNTROWS(Objects)
        formatString: #,##0

    measure One = 1
        formatString: 0

table Reference
    annotation Weaver.Source = Warehouse/_weaver/_.TableDictionary

relationship ObjectNames
    fromColumn: Objects.'Item name'
    toColumn: Reference.'Item name'
    fromCardinality: many
    toCardinality: many
    crossFilteringBehavior: bothDirections

table Metric
    isHidden
    annotation Weaver.MeasureTable = true

    measure Value
        annotation Weaver.Switch = ```
            Objects[Rows]
            Objects[One]
            ```

table Obsolete
    annotation Weaver.Exclude = true
    measure Old = 1
"""
METRIC_TEXT = """table Metric
    isHidden
    annotation Weaver.MeasureTable = true

    measure Value
        annotation Weaver.Switch = Sales[Revenue]
"""
OVERLAY_TEXT = """model Model
    annotation Weaver.AutoHideColumns = "Amount"
    annotation Weaver.AutoHideForeignKeys = true

ref table Sales
    column Id
        annotation Weaver.Exclude = true
"""


def annotation_project(folder, form):
    """Write the annotation project in one of its forms.

    ``source-extension`` generates its tables from the catalogue, ``pbip``
    annotates the Probe PBIP, and ``overlays`` annotates it from extensions.
    """

    folder.mkdir(parents=True)
    if form == "source-extension":
        (folder / f"{folder.name}.tmdl").write_text(SOURCE_TEXT, encoding="utf-8")
        return
    shutil.copytree(PBIP, folder, dirs_exist_ok=True)
    definition = folder / "Probe.SemanticModel/definition"
    if form == "pbip":
        model = definition / "model.tmdl"
        model.write_text(
            model.read_text(encoding="utf-8").replace(
                "\nref table Sales",
                "\n\tannotation Weaver.AutoHideColumns = Amount\n\tannotation Weaver.AutoHideForeignKeys = true\n\nref table Sales",
                1,
            ),
            encoding="utf-8",
        )
        sales = definition / "tables/Sales.tmdl"
        sales.write_text(
            sales.read_text(encoding="utf-8").replace(
                "\n\tcolumn ProductId",
                "\n\t\tannotation Weaver.Exclude = true\n\n\tcolumn ProductId",
                1,
            ),
            encoding="utf-8",
        )
        (definition / "tables/Metric.tmdl").write_text(METRIC_TEXT, encoding="utf-8")
        return
    policy_path(folder.parent.parent).write_text(
        'model Model\n\tannotation Weaver.AutoHideColumns = "Product*"\n',
        encoding="utf-8",
    )
    (folder / f"{folder.name}.tmdl").write_text(
        OVERLAY_TEXT + "\n" + METRIC_TEXT, encoding="utf-8"
    )
