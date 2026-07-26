"""Schema ``_`` is an ordinary schema, and an object may depend on nothing.

Weaver's own catalogue lives in schema ``_`` of the Weaver Lakehouse, declared as
ordinary SES and built through the ordinary build path. Two rules had to give way
for that, and both were over-broad rather than load-bearing:

- the underscore convention is about *directories* — ``_schemas``, ``_helpers`` —
  so a root file may begin with one when it still names a schema and an object;
- a Spark SQL object must be explicit about its dependencies, which an empty list
  satisfies as well as a populated one. A query built from literals has none.

A private root file is still private, and a misnamed object file is still an
error rather than quietly demoted to support.
"""

from __future__ import annotations

import pytest

from weaver import LocalStore, Location
from weaver.errors import DiscoveryError, MetadataError
from weaver.ses import PYTHON, SPARK_SQL, parse_document, read_repository

REGISTRY = """\
/*
Table ID: _.Registry

Description: Objects Weaver currently certifies as installed.

Lineage: Maintained by Weaver's own build, never loaded.

Dependencies: []

Static: true

Prohibit rebuild: true

Schema:
  repository: string
  schema_name: string
  object_name: string
*/
select cast(null as string) as repository
     , cast(null as string) as schema_name
     , cast(null as string) as object_name
 where 1 = 0
"""


def _python_object(qualified: str, *, extra: str = "", imports: str = "") -> str:
    """One Python Delta table, metadata in the module docstring where it belongs.

    ``read()`` raises: build creates structure and never calls it, which is the
    invariant these fixtures are free to rely on.
    """

    class_name = qualified.replace(".", "__")
    return (
        f'"""\nTable ID: {qualified}\n\n'
        f"Description: {qualified} for a discovery test.\n\n"
        f"Lineage: Declared for a test.\n\n"
        f"{extra}"
        f"Schema:\n  Order id: string\n"
        f'"""\n\n'
        f"{imports}"
        f"from weaver import Table\n\n\n"
        f"class {class_name}(Table):\n"
        f"    def read(self):\n"
        f"        raise NotImplementedError\n"
    )


def _repo(tmp_path, files: dict[str, str], schemas=("_",)):
    root = tmp_path / "repo"
    root.mkdir()
    (root / "_schemas").mkdir()
    for schema in schemas:
        (root / "_schemas" / f"{schema}.yml").write_text(
            f"Schema ID: {schema}\n", encoding="utf-8"
        )
    for name, text in files.items():
        (root / name).write_text(text, encoding="utf-8")
    return read_repository(Location(value=str(root)), store=LocalStore(), name="repo")


def test_an_object_in_schema_underscore_is_read_as_an_object(tmp_path):
    repo = _repo(tmp_path, {"_.Registry.spark.sql": REGISTRY})
    document = repo["_.Registry"]
    assert document.object_id.schema == "_"
    assert document.object_id.object == "Registry"
    assert document.node_id == "delta:_.Registry"
    assert repo.support_files == ()


def test_the_underscore_schema_must_still_be_declared(tmp_path):
    with pytest.raises(DiscoveryError, match="undeclared schema"):
        _repo(tmp_path, {"_.Registry.spark.sql": REGISTRY}, schemas=("Sales",))


def test_a_private_root_file_is_still_support_not_an_object(tmp_path):
    repo = _repo(
        tmp_path,
        {"_.Registry.spark.sql": REGISTRY, "_scratch.py": "not an object\n"},
    )
    assert repo.support_files == ("_scratch.py",)
    assert [document.qualified for document in repo.documents] == ["_.Registry"]


def test_a_misnamed_object_file_is_an_error_not_quietly_demoted(tmp_path):
    # No leading underscore, so it is judged an object file on its suffix and its
    # stem is then reported as wrong. Demoting it to support would hide a typo.
    with pytest.raises(DiscoveryError, match="separates schema and object"):
        _repo(tmp_path, {"Sales.Order.py": "class X: pass\n"}, schemas=("Sales",))


def test_an_underscored_file_that_names_nothing_is_support(tmp_path):
    # Python cannot express schema `_`: `_` + `__` + `Registry` is `___Registry`,
    # whose first part is empty. So it is support, which is the honest answer.
    repo = _repo(
        tmp_path,
        {"_.Registry.spark.sql": REGISTRY, "___Registry.py": "class X: pass\n"},
    )
    assert repo.support_files == ("___Registry.py",)


# --- an explicit absence of dependencies ------------------------------------


def _registry_metadata(*, with_dependencies: bool = True) -> str:
    from weaver.ses import extract_sql_metadata_and_body

    metadata, _body = extract_sql_metadata_and_body(REGISTRY)
    if with_dependencies:
        return metadata
    return "\n".join(
        line for line in metadata.splitlines() if line != "Dependencies: []"
    )


def test_an_empty_dependencies_list_is_an_explicit_none():
    document = parse_document(_registry_metadata(), language=SPARK_SQL)
    assert document.dependencies == ()
    assert document.declares_dependencies


def test_a_spark_sql_object_must_still_say_something():
    with pytest.raises(MetadataError, match=r"Dependencies: \[\]"):
        parse_document(_registry_metadata(with_dependencies=False), language=SPARK_SQL)


def test_an_explicit_none_suppresses_discovery(tmp_path):
    """`Dependencies: []` replaces discovery rather than falling back to it.

    A declaration has always replaced discovery so an author can *remove* an
    edge; an empty declaration is the same statement about the empty set. Without
    this, `Dependencies: []` would silently mean "discover them for me".
    """

    from weaver.ses import effective_dependencies

    consumer = _python_object(
        "Sales.Ignored", extra="Dependencies: []\n", imports="from Sales__Order import Sales__Order\n"
    )
    parent = _python_object("Sales.Order")
    repo = _repo(
        tmp_path,
        {"Sales__Ignored.py": consumer, "Sales__Order.py": parent},
        schemas=("Sales",),
    )
    document = repo["Sales.Ignored"]
    assert document.referenced_object_ids  # discovery did see the import
    assert effective_dependencies(document) == ()
    assert repo.dependency_graph.upstream_of("delta:Sales.Ignored") == ()


def test_a_python_object_without_the_key_still_discovers_its_imports(tmp_path):
    from weaver.ses import effective_dependencies

    consumer = _python_object(
        "Sales.Derived", imports="from Sales__Order import Sales__Order\n"
    )
    parent = _python_object("Sales.Order")
    repo = _repo(
        tmp_path,
        {"Sales__Derived.py": consumer, "Sales__Order.py": parent},
        schemas=("Sales",),
    )
    assert [str(dependency) for dependency in
            effective_dependencies(repo["Sales.Derived"])] == ["Sales.Order"]


def test_the_retired_audit_spelling_is_still_reserved():
    from weaver.ses import parse_document as parse

    base = """
Table ID: Sales.Order

Description: Orders.

Lineage: Nothing.

Schema:
  Order id: string
"""
    for spelling in ("Row_insert_datetime", "row_insert_datetime", "Row insert datetime"):
        with pytest.raises(MetadataError, match="reserved for Weaver's audit columns"):
            parse(base + f"  {spelling}: timestamp\n", language=PYTHON)
