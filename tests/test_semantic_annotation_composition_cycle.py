"""Named raw composition enters the phase dispatcher only for selected models."""

from support.weaver_test import weaver_test
from test_powerbi_project_declaration import write


@weaver_test()
def test_composition_directive_never_executes_and_inherited_phases_run_once(
    tmp_path, monkeypatch
):
    from weaver.declaration.model import WeaverItemId
    from weaver.declaration.repository import parse_item_repository
    from weaver.locations import Location
    from weaver.semantic_models import TmdlDefinition
    from weaver.semantic_models.binding import bind_semantic_sources
    from weaver.semantic_models.builtin_annotations import Weaver__BaseSemanticModels

    def forbidden(self, target):
        raise AssertionError("composition directive executed as an annotation")

    monkeypatch.setattr(Weaver__BaseSemanticModels, "apply", forbidden)
    write(
        tmp_path,
        "PowerBI/Sales/Base.tmdl",
        "model Model\n\tannotation Acme.Schema = true\n\tannotation Acme.Post = true\n",
    )
    write(
        tmp_path,
        "PowerBI/Sales/Derived.tmdl",
        "model Model\n\tannotation Weaver.BaseSemanticModels = Base\n",
    )
    write(
        tmp_path,
        "PowerBI/policy.tmdl",
        "model Model\n\tannotation Acme.Policy = true\n",
    )
    write(
        tmp_path,
        "PowerBI/annotations/Acme__Schema.py",
        """
from weaver.semantic_models import Annotation
class Acme__Schema(Annotation):
    scopes = {"model"}
    phase = "schema"
    def apply(self, target):
        target.description = (target.description or "") + "|schema"
        table = target.tables.add("Generated")
        table.columns.add("Id", dataType="int64")
""",
    )
    write(
        tmp_path,
        "PowerBI/annotations/Acme__Post.py",
        """
from weaver.semantic_models import Annotation
class Acme__Post(Annotation):
    scopes = {"model"}
    def apply(self, target):
        target.description = (target.description or "") + "|post"
        target.tables["Generated"].columns["Id"].description = "post seen"
""",
    )
    write(
        tmp_path,
        "PowerBI/annotations/Acme__Policy.py",
        """
from weaver.semantic_models import Annotation
class Acme__Policy(Annotation):
    scopes = {"model"}
    def apply(self, target):
        target.description = (target.description or "") + "|policy"
""",
    )
    derived = WeaverItemId("SemanticModel", "Derived")
    base = WeaverItemId("SemanticModel", "Base")
    raw = parse_item_repository(Location(tmp_path.as_posix()))
    base_parts = dict(raw.semantic_models[base].parts)
    compiled = bind_semantic_sources(raw, {}, {derived})
    assert compiled.semantic_models[base].parts == base_parts
    assert TmdlDefinition(base_parts).model.description is None
    contribution = compiled.semantic_models[derived]
    assert any(
        b"annotation 'Weaver.BaseSemanticModels' = Base" in content
        for content in contribution.parts.values()
    )
    assert {a["name"]: a["value"] for a in contribution.requested["annotations"]}[
        "Weaver.BaseSemanticModels"
    ] == "Base"
    model = TmdlDefinition(contribution.parts).model
    assert model.description.count("|schema") == 1
    assert model.description.count("|post") == 1
    assert model.description.count("|policy") == 1
    assert model.description.startswith("|schema")
    assert model.tables["Generated"].columns["Id"].description == "post seen"
    assert contribution.table_order == ("Generated",)
    assert (
        contribution.requested["tables"][0]["columns"][0]["description"] == "post seen"
    )
