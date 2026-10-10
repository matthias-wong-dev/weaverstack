"""Public annotation names, scopes and value grammars are enforced together."""

import pytest
from support.weaver_test import weaver_test
from test_semantic_annotation_declaration import compile_source, source_project
from test_semantic_annotation_origins_representation import NAMES
from test_semantic_annotation_representation import extension_model, switch_model

from weaver.errors import ConfigError


@weaver_test()
def test_switch_rejects_duplicate_selector_labels_from_qualified_measures(tmp_path):
    with pytest.raises(ConfigError, match="(?i)ambiguous.*selector"):
        compile_source(
            switch_model(
                tmp_path, ["Sales[Revenue]", "Finance[Revenue]"], duplicate=True
            )
        )


@weaver_test()
def test_switch_rejects_a_reference_to_its_own_measure(tmp_path):
    with pytest.raises(ConfigError, match="(?i)recursive|switch.*reference"):
        compile_source(switch_model(tmp_path, ["Metric[Value]"]))


@weaver_test()
def test_measure_table_does_not_silently_add_to_an_authored_partition(tmp_path):
    root = extension_model(
        tmp_path,
        "table Metric\n\tannotation Weaver.MeasureTable = true\n"
        '\tpartition Old = calculated\n\t\tsource = ROW("Value", 1)\n',
    )
    with pytest.raises(ConfigError, match="(?i)MeasureTable.*partition"):
        compile_source(root)


@pytest.mark.parametrize(
    "name,text",
    [
        (
            "Weaver.Source",
            "model Model\n\tannotation Weaver.Source = Warehouse/Serving/Cake.Sales\n",
        ),
        (
            "Weaver.MeasureTable",
            "table Sales\n\tmeasure Revenue = 1\n\t\tannotation Weaver.MeasureTable = true\n",
        ),
        ("Weaver.Switch", "table Sales\n\tannotation Weaver.Switch = Sales[Revenue]\n"),
        (
            "Weaver.AutoHideColumns",
            'table Sales\n\tmeasure Revenue = 1\n\t\tannotation Weaver.AutoHideColumns = "*SK"\n',
        ),
        (
            "Weaver.AutoHideForeignKeys",
            "table Sales\n\tannotation Weaver.AutoHideForeignKeys = true\n",
        ),
        ("Weaver.Exclude", "model Model\n\tannotation Weaver.Exclude = true\n"),
    ],
)
@weaver_test()
def test_each_annotation_rejects_an_invalid_scope(tmp_path, name, text):
    with pytest.raises(ConfigError, match=rf"{name}.*scope"):
        compile_source(extension_model(tmp_path, text))


@pytest.mark.parametrize("name", NAMES)
@weaver_test()
def test_each_annotation_validates_its_value_grammar(tmp_path, name):
    if name == "Weaver.Switch":
        root = switch_model(tmp_path, [])
    elif name == "Weaver.Source":
        root = source_project(tmp_path, value="invalid")
    else:
        scope = "model Model" if name == "Weaver.AutoHideForeignKeys" else "table Sales"
        value = '""' if name == "Weaver.AutoHideColumns" else "invalid"
        root = extension_model(tmp_path, f"{scope}\n\tannotation {name} = {value}\n")
    with pytest.raises(ConfigError, match=name):
        compile_source(root)


@pytest.mark.parametrize("name", ["Weaver.Soruce", "weaver.Source", "WEAVER.Source"])
@weaver_test()
def test_reserved_namespace_typos_fail_without_normalising_the_name(tmp_path, name):
    with pytest.raises(ConfigError, match=name):
        compile_source(source_project(tmp_path, annotation=name))
