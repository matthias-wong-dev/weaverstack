"""Project annotation classes compile through the same framework as Weaver's own."""

import sys
import textwrap

import pytest
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import ITEM
from test_semantic_annotation_origins_representation import NAMES

from weaver.declaration.repository import parse_item_repository
from weaver.errors import ConfigError
from weaver.locations import Location
from weaver.semantic_models import Annotation, TmdlObject
from weaver.semantic_models.annotation import annotation_name, builtin_registry

HIDE_INTEGERS = """
from weaver.semantic_models import Annotation


class DWG__HideIntegerColumns(Annotation):
    \"\"\"Hide every int64 column in scope.\"\"\"

    scopes = {"model", "table"}

    def apply(self, target):
        tables = target.tables if target.parent is None else [target]
        for table in tables:
            for column in table.columns:
                if column.dataType == "int64":
                    column.isHidden = True
"""
TABLES = (
    "table Sales\n\tcolumn Quantity\n\t\tdataType: int64\n"
    "\tcolumn Region\n\t\tdataType: string\n\n"
    "table Customer\n\tcolumn CustomerId\n\t\tdataType: int64\n"
)


def project(tmp_path, extension, annotations=None, *, name="project"):
    root = tmp_path / name
    folder = root / str(ITEM)
    folder.mkdir(parents=True)
    (folder / f"{folder.name}.tmdl").write_text(extension, encoding="utf-8")
    for stem, source in (annotations or {}).items():
        path = root / "PowerBI/annotations" / f"{stem}.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(source), encoding="utf-8")
    return root


def parse(root):
    from support.semantic_compilation import compile_repository

    return compile_repository(parse_item_repository(Location(root.as_posix())))


def compile_model(root):
    return parse(root).semantic_models[ITEM]


def column_hidden(contribution, table, column):
    from weaver.semantic_models import TmdlDefinition

    model = TmdlDefinition(contribution.parts).model
    return model.tables[table].columns[column].isHidden


@weaver_test()
def test_builtins_are_annotation_classes_named_by_the_same_convention():
    registry = builtin_registry()
    assert set(registry.classes) == set(NAMES) | {"Weaver.BaseSemanticModels"}
    for name, cls in registry.classes.items():
        assert issubclass(cls, Annotation)
        assert annotation_name(cls) == name
        assert cls.scopes and cls.__doc__
        assert cls.apply is not Annotation.apply


@weaver_test()
def test_builtins_are_dispatched_through_apply(tmp_path, monkeypatch):
    from weaver.semantic_models.builtin_annotations import Weaver__AutoHideColumns

    calls = []
    original = Weaver__AutoHideColumns.apply

    def spy(self, target):
        calls.append((type(self), target))
        return original(self, target)

    monkeypatch.setattr(Weaver__AutoHideColumns, "apply", spy)
    root = project(
        tmp_path,
        'model Model\n\tannotation Weaver.AutoHideColumns = "*Id"\n\n' + TABLES,
    )
    contribution = compile_model(root)
    assert [(cls, type(target)) for cls, target in calls] == [
        (Weaver__AutoHideColumns, TmdlObject)
    ]
    assert column_hidden(contribution, "Customer", "CustomerId") is True


@weaver_test()
def test_model_scoped_project_annotation_edits_native_properties(tmp_path):
    root = project(
        tmp_path,
        "model Model\n\tannotation DWG.HideIntegerColumns = true\n\n" + TABLES,
        {"DWG__HideIntegerColumns": HIDE_INTEGERS},
    )
    repository = parse(root)
    contribution = compile_model(root)
    assert column_hidden(contribution, "Sales", "Quantity") is True
    assert column_hidden(contribution, "Customer", "CustomerId") is True
    assert column_hidden(contribution, "Sales", "Region") is None
    assert (
        b"annotation 'DWG.HideIntegerColumns' = true"
        in (contribution.parts["definition/model.tmdl"])
    )
    sales = next(t for t in contribution.requested["tables"] if t["name"] == "Sales")
    assert sales["columns"][0] == {
        "name": "Quantity",
        "dataType": "int64",
        "isHidden": True,
    }
    assert contribution.provenance["/model/tables/Sales/columns/Quantity/isHidden"][
        "reason"
    ] == ("DWG.HideIntegerColumns")
    assert {
        i.identity for i in repository.items if i.identity.item_type == "SemanticModel"
    } == {ITEM}


@weaver_test()
def test_table_scope_acts_only_on_the_annotated_table(tmp_path):
    root = project(
        tmp_path,
        TABLES.replace(
            "table Customer\n",
            "table Customer\n\tannotation DWG.HideIntegerColumns = true\n",
        ),
        {"DWG__HideIntegerColumns": HIDE_INTEGERS},
    )
    contribution = compile_model(root)
    assert column_hidden(contribution, "Customer", "CustomerId") is True
    assert column_hidden(contribution, "Sales", "Quantity") is None


@weaver_test()
def test_value_helpers_and_located_errors(tmp_path):
    source = """
        from weaver.semantic_models import Annotation

        class Acme__Finance__Currency(Annotation):
            scopes = {"column"}

            def apply(self, target):
                if self.value not in {"AUD", "NZD"}:
                    self.error(f"unsupported currency {self.value}")
                target.formatString = self.value + " #,##0.00"
                target.displayFolder = ", ".join(self.lines())
    """
    root = project(
        tmp_path,
        'table Sales\n\tcolumn Amount\n\t\tannotation Acme.Finance.Currency = "AUD"\n',
        {"Acme__Finance__Currency": source},
    )
    contribution = compile_model(root)
    assert (
        b"\t\tformatString: AUD #,##0.00\n\t\tdisplayFolder: AUD\n"
        in (contribution.parts["definition/tables/Sales.tmdl"])
    )
    (root / str(ITEM) / f"{ITEM.item_name}.tmdl").write_text(
        "table Sales\n\tcolumn Amount\n\t\tannotation Acme.Finance.Currency = USD\n"
    )
    with pytest.raises(
        ConfigError,
        match=r"^SemanticModel/Reporting/Reporting.tmdl:3: Acme.Finance.Currency: "
        "unsupported currency USD$",
    ):
        compile_model(root)


@pytest.mark.parametrize(
    "name,defined",
    [("DWG.DoesNotExist", True), ("Weaver.DoesNotExist", False)],
)
@weaver_test()
def test_unknown_annotation_in_a_defined_namespace_fails(tmp_path, name, defined):
    root = project(
        tmp_path,
        f"model Model\n\tannotation {name} = true\n",
        {"DWG__HideIntegerColumns": HIDE_INTEGERS} if defined else None,
    )
    with pytest.raises(ConfigError, match=rf"{name}: unknown \w+ annotation"):
        compile_model(root)


@weaver_test()
def test_annotations_in_an_undefined_namespace_stay_native(tmp_path):
    root = project(tmp_path, "model Model\n\tannotation DWG.DoesNotExist = true\n")
    contribution = compile_model(root)
    assert (
        b"annotation DWG.DoesNotExist = true"
        in (contribution.parts["definition/model.tmdl"])
    )


@pytest.mark.parametrize(
    "stem,source,diagnostic",
    [
        (
            "Weaver__Foo",
            "class Weaver__Foo(Annotation):\n    scopes = {'model'}\n"
            "    def apply(self, target): pass\n",
            "Weaver namespace is reserved",
        ),
        (
            "DWG__Foo",
            "class DWG__Bar(Annotation):\n    scopes = {'model'}\n"
            "    def apply(self, target): pass\n",
            "rename class DWG__Bar to DWG__Foo",
        ),
        ("DWG__Foo", "x = 1\n", "exactly one Annotation subclass.*found none"),
        (
            "DWG__Foo",
            "class DWG__Foo(Annotation):\n    scopes = {'model'}\n"
            "    def apply(self, target): pass\n"
            "class DWG__Other(Annotation):\n    pass\n",
            "found DWG__Foo, DWG__Other",
        ),
        (
            "Foo",
            "class Foo(Annotation):\n    scopes = {'model'}\n"
            "    def apply(self, target): pass\n",
            "<Namespace>__<Name>",
        ),
        (
            "DWG__Foo",
            "class DWG__Foo(Annotation):\n    def apply(self, target): pass\n",
            "set DWG__Foo.scopes",
        ),
        (
            "DWG__Foo",
            "class DWG__Foo(Annotation):\n    scopes = {'model'}\n",
            "must implement apply",
        ),
        ("DWG__Foo", "raise ValueError('broken')\n", "ValueError: broken"),
        (
            "DWG__Foo",
            "class DWG__Foo(Annotation):\n    scopes = {'table', 'tabel'}\n"
            "    def apply(self, target): pass\n",
            "DWG__Foo.scopes names 'tabel', which is not a TMDL object kind. Use "
            "annotation, column,",
        ),
    ],
)
@weaver_test()
def test_invalid_annotation_files_fail_at_discovery(tmp_path, stem, source, diagnostic):
    root = project(
        tmp_path,
        "model Model\n",
        {stem: "from weaver.semantic_models import Annotation\n" + source},
    )
    with pytest.raises(
        ConfigError, match=rf"PowerBI/annotations/{stem}.py: .*{diagnostic}"
    ):
        parse(root)


@weaver_test()
def test_project_annotations_do_not_leak_between_repositories(tmp_path):
    defining = project(
        tmp_path,
        "model Model\n\tannotation DWG.HideIntegerColumns = true\n\n" + TABLES,
        {"DWG__HideIntegerColumns": HIDE_INTEGERS},
        name="a",
    )
    other = project(
        tmp_path,
        "model Model\n\tannotation DWG.HideIntegerColumns = true\n\n" + TABLES,
        name="b",
    )
    modules = set(sys.modules)
    assert column_hidden(compile_model(defining), "Sales", "Quantity") is True
    plain = compile_model(other)
    assert column_hidden(plain, "Sales", "Quantity") is None
    assert (
        b"annotation DWG.HideIntegerColumns = true"
        in (plain.parts["definition/model.tmdl"])
    )
    assert set(sys.modules) == modules


@weaver_test()
def test_implementation_change_recompiles_without_changing_desired_state(tmp_path):
    root = project(
        tmp_path,
        "model Model\n\tannotation DWG.HideIntegerColumns = true\n\n" + TABLES,
        {"DWG__HideIntegerColumns": HIDE_INTEGERS},
    )
    first = parse(root)
    first_compiled = compile_model(root)
    refactored = HIDE_INTEGERS.replace(
        '                if column.dataType == "int64":\n'
        "                    column.isHidden = True\n",
        '                column.isHidden = column.dataType == "int64" or None\n',
    )
    assert refactored != HIDE_INTEGERS
    path = root / "PowerBI/annotations/DWG__HideIntegerColumns.py"
    path.write_text(textwrap.dedent(refactored), encoding="utf-8")
    second = parse(root)
    second_compiled = compile_model(root)
    assert second.signature != first.signature
    assert second_compiled.signature == first_compiled.signature
    path.write_text(
        textwrap.dedent(HIDE_INTEGERS.replace('"int64"', '"string"')), encoding="utf-8"
    )
    changed = compile_model(root)
    assert changed.signature != first_compiled.signature


@weaver_test()
def test_project_annotations_are_not_a_power_bi_project(tmp_path):
    root = project(
        tmp_path, "model Model\n", {"DWG__HideIntegerColumns": HIDE_INTEGERS}
    )
    repository = parse(root)
    assert "annotations" not in repository.powerbi_projects
    assert {item.identity for item in repository.items} >= {ITEM}


@weaver_test()
def test_an_annotation_file_under_semantic_model_is_refused(tmp_path):
    root = project(tmp_path, "model Model\n")
    outside = root / "SemanticModel/annotations/DWG__HideIntegerColumns.py"
    outside.parent.mkdir(parents=True)
    outside.write_text(textwrap.dedent(HIDE_INTEGERS), encoding="utf-8")
    with pytest.raises(ConfigError, match="SemanticModel/annotations/DWG__Hide"):
        parse_item_repository(Location(root.as_posix()))


@weaver_test()
def test_a_table_name_with_a_space_cites_the_authored_file(tmp_path):
    import re

    root = project(
        tmp_path, "table 'Notice SQL'\n\tannotation Weaver.Exclude = maybe\n"
    )
    with pytest.raises(
        ConfigError,
        match="^"
        + re.escape(
            "SemanticModel/Reporting/Reporting.tmdl:2: Weaver.Exclude: requires a "
            "boolean true or false"
        )
        + "$",
    ):
        compile_model(root)


@weaver_test()
def test_an_exception_inside_apply_names_the_annotation_and_declaration(tmp_path):
    import re

    source = """
        from weaver.semantic_models import Annotation

        class Acme__Broken(Annotation):
            scopes = {"table"}

            def apply(self, target):
                target.columns["No such column"]
    """
    root = project(
        tmp_path,
        "table Sales\n\tannotation Acme.Broken = true\n\tcolumn Id\n",
        {"Acme__Broken": source},
    )
    with pytest.raises(
        ConfigError,
        match="^"
        + re.escape(
            "SemanticModel/Reporting/Reporting.tmdl:2: Acme.Broken: Acme__Broken "
            "(PowerBI/annotations/Acme__Broken.py) raised KeyError: "
        )
        + ".*No such column",
    ) as caught:
        compile_model(root)
    assert isinstance(caught.value.__cause__, KeyError)
