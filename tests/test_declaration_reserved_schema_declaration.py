"""An underscore schema is ordinary, and an object may depend on nothing.

Weaver's own catalogue lives in schema ``_`` of a Warehouse, declared as
ordinary Weaver document and built through the ordinary build path. Two rules had to give way
for that, and both were over-broad rather than load-bearing:

- the underscore convention is about directories, ``schemas/``, ``lib/``, so
  a document filename may begin with one when it still names a schema and an
  object;
- a Spark SQL object must be explicit about its dependencies, which an empty list
  satisfies as well as a populated one. A query built from literals has none.

``_`` itself is the one name an ordinary item may not use, and that is a
statement about ownership rather than a return of the naming rule: it holds the
runtime tree a load is deployed into and the schema generated load procedures
live in, both of which Weaver generates and prunes. The package-owned
``Warehouse/_weaver`` still declares its catalogue there, because it is the item
that owns it. Any other underscore schema is authored freely, which is what these
tests check.

A misnamed object file is still an error rather than demoted to
support.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

from weaver.declaration import PYTHON, SPARK_SQL, parse_document, parse_item_repository
from weaver.declaration.model import WeaverDocumentId
from weaver.errors import DiscoveryError, MetadataError
from weaver.locations import Location
from weaver.store import FilesystemStore

REGISTRY = """\
/*
Table ID: _Control.Registry

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
 where 1 = 0;
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


ITEM = "Lakehouse/Raw"


class _Documents:
    """The item's documents, addressed by their local ``Schema.Object`` name."""

    def __init__(self, repository):
        self.repository = repository
        self.support_files = tuple(
            path for path in repository.support_files if path.startswith(f"{ITEM}/")
        )
        self.dependency_graph = repository.dependency_graph
        self.documents = tuple(
            document
            for identity, document in repository.source_documents.items()
            if str(identity.item) == ITEM
        )

    def __getitem__(self, qualified: str):
        return self.repository.source_documents[
            WeaverDocumentId.parse(f"{ITEM}/{qualified}")
        ]


def _repo(tmp_path, files: dict[str, str], schemas=("_Control",)):
    root = tmp_path / "repo"
    for schema in schemas:
        path = root / ITEM / "schemas" / f"{schema}.yml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"Schema ID: {schema}\n", encoding="utf-8")
    for name, text in files.items():
        path = root / ITEM / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return _Documents(
        parse_item_repository(Location(value=str(root)), store=FilesystemStore())
    )


@weaver_test()
def test_an_object_in_an_underscore_schema_is_read_as_an_object(tmp_path):
    repo = _repo(tmp_path, {"_Control.Registry.sql": REGISTRY})
    document = repo["_Control.Registry"]
    assert document.object_id.schema == "_Control"
    assert document.object_id.object == "Registry"
    assert document.node_id == f"{ITEM}/_Control.Registry"
    assert repo.support_files == ()


@weaver_test()
def test_an_underscore_schema_must_still_be_declared(tmp_path):
    with pytest.raises(DiscoveryError, match="is not declared by item"):
        _repo(tmp_path, {"_Control.Registry.sql": REGISTRY}, schemas=("Sales",))


@weaver_test()
def test_schema_underscore_itself_belongs_to_weaver(tmp_path):
    """The one name an ordinary item may not author into.

    It holds the deployed runtime tree, the generated procedures and the item's
    surface over the catalogue, so an authored object there would collide with
    something Weaver claims and prunes. Refused at interpretation, where the
    author can still see which file caused it.
    """

    registry = REGISTRY.replace("_Control.Registry", "_.Registry")
    with pytest.raises(DiscoveryError, match="reserved for Weaver"):
        _repo(tmp_path, {"_.Registry.sql": registry}, schemas=("_",))


@weaver_test()
def test_a_misnamed_object_file_is_an_error_not_quietly_demoted(tmp_path):
    # No leading underscore, so it is judged an object file on its suffix and its
    # stem is then reported as wrong. Demoting it to support would hide a typo.
    with pytest.raises(DiscoveryError, match="separates schema and object"):
        _repo(tmp_path, {"Sales.Order.py": "class X: pass\n"}, schemas=("Sales",))


# --- an explicit absence of dependencies ------------------------------------


def _registry_metadata(*, with_dependencies: bool = True) -> str:
    from weaver.declaration import extract_sql_metadata_and_body

    metadata, _body = extract_sql_metadata_and_body(REGISTRY)
    if with_dependencies:
        return metadata
    return "\n".join(
        line for line in metadata.splitlines() if line != "Dependencies: []"
    )


@weaver_test()
def test_an_empty_dependencies_list_is_an_explicit_none():
    document = parse_document(_registry_metadata(), language=SPARK_SQL)
    assert document.dependencies == ()
    assert document.declares_dependencies


@weaver_test()
def test_a_spark_sql_object_must_still_say_something():
    with pytest.raises(MetadataError, match=r"Dependencies: \[\]"):
        parse_document(_registry_metadata(with_dependencies=False), language=SPARK_SQL)


@weaver_test()
def test_an_explicit_none_suppresses_discovery(tmp_path):
    """`Dependencies: []` replaces discovery rather than falling back to it.

    A declaration has always replaced discovery so an author can remove an
    edge; an empty declaration is the same statement about the empty set. Without
    this, `Dependencies: []` would silently mean "discover them for me".
    """

    from weaver.declaration import effective_dependencies

    consumer = _python_object(
        "Sales.Ignored",
        extra="Dependencies: []\n",
        imports="from Sales__Order import Sales__Order\n",
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
    assert repo.dependency_graph.upstream_of(f"{ITEM}/Sales.Ignored") == ()


@weaver_test()
def test_a_python_object_without_the_key_still_discovers_its_imports(tmp_path):
    from weaver.declaration import effective_dependencies

    consumer = _python_object(
        "Sales.Derived", imports="from Sales__Order import Sales__Order\n"
    )
    parent = _python_object("Sales.Order")
    repo = _repo(
        tmp_path,
        {"Sales__Derived.py": consumer, "Sales__Order.py": parent},
        schemas=("Sales",),
    )
    assert [
        str(dependency) for dependency in effective_dependencies(repo["Sales.Derived"])
    ] == ["Sales.Order"]


@weaver_test()
def test_the_retired_audit_spelling_is_still_reserved():
    from weaver.declaration import parse_document as parse

    base = """
Table ID: Sales.Order

Description: Orders.

Lineage: Nothing.

Schema:
  Order id: string
"""
    for spelling in (
        "Row_insert_datetime",
        "row_insert_datetime",
        "Row insert datetime",
    ):
        with pytest.raises(MetadataError, match="reserved for Weaver's audit columns"):
            parse(base + f"  {spelling}: timestamp\n", language=PYTHON)
