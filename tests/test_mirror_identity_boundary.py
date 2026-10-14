"""Mirror publication uses resolved destination identities."""

from copy import deepcopy
from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from test_mirror_plan_boundary import PUBLISH, _actions, _resolved, _Session

from weaver.catalogue.borrow import Borrowed
from weaver.catalogue.builtin import BUILTIN_ITEM
from weaver.catalogue.render import InstallationScope, render_merge
from weaver.catalogue.tables import INSTALLATION
from weaver.declaration.model import WeaverDocumentId
from weaver.fabric.resolution import FabricResolver
from weaver.mirror_plan import mirror_mutation_plan
from weaver.store import FilesystemStore
from weaver.targets import ItemRef

# Recorded Fabric identifiers, with neutral display names.
WORKSPACE_ID = "bc690e88-80e3-4e97-8184-f919b46b91cf"
ITEMS = (
    ("Warehouse", "Weaver", "dab72240-56f1-4b03-8105-5a41d1401721"),
    ("Warehouse", "Weaver_Dev", "465bc1d2-4900-4644-b99c-e706f5f0fff2"),
    ("Warehouse", "Model", "6ec437ec-e225-48bb-857a-8211746b62f4"),
    ("Warehouse", "Model_Dev", "d0b275ae-6e3e-4252-82fa-f0d9396a6850"),
    ("Lakehouse", "Input", "83ec06ff-846f-4a6e-a83f-17c5805a7b6a"),
    ("Lakehouse", "Input_Dev", "0e4e9c91-75be-41b7-8c9a-8d6fcc2a5f6b"),
)


class RecordedInventory:
    def __init__(self):
        self.requested = []

    def paged(self, path, **_):
        self.requested.append(path)
        if path == "workspaces":
            return [{"id": WORKSPACE_ID, "displayName": "Analytics"}]
        route, _, query = path.partition("?")
        assert route == f"workspaces/{WORKSPACE_ID}/items"
        kind = query.partition("type=")[2]
        return [
            {"id": item_id, "displayName": name, "type": item_type}
            for item_type, name, item_id in ITEMS
            if not kind or kind == item_type
        ]


def recorded_session(resolved, tmp_path):
    client = RecordedInventory()
    resolver = FabricResolver(
        resolved.workspace, client=client, base_url=tmp_path.as_posix()
    )
    resolver.discover()
    return _Session(resolver=resolver, store=FilesystemStore()), client


@weaver_test()
def test_catalogue_only_fork_bootstrap_publishes_resolved_identity(tmp_path):
    resolved = _resolved("Model", "Warehouse")
    resolved = replace(
        resolved, plan=replace(resolved.plan, items=()), items=(), installations={}
    )
    session, client = recorded_session(resolved, tmp_path)
    plan, payloads, _ = mirror_mutation_plan(resolved, session=session)
    bootstrap = payloads[_actions(plan)["publish-catalogue"].payload].decode()
    assert WORKSPACE_ID in bootstrap
    assert ITEMS[1][2] in bootstrap
    assert "[Workspace name]" in bootstrap
    assert "[Target item GUID]" in bootstrap
    builtin = next(
        target
        for target in plan.targets
        if target.logical_item_name == BUILTIN_ITEM.item_name
    )
    assert (builtin.workspace_id, builtin.item_id) == (WORKSPACE_ID, ITEMS[1][2])
    assert builtin.name == "Weaver_Dev"
    assert ITEMS[0][2] not in bootstrap
    assert client.requested == ["workspaces", f"workspaces/{WORKSPACE_ID}/items"]
    assert (
        "[Weaver].[_].[Installation]"
        in payloads[_actions(plan)[PUBLISH].payload].decode()
    )


@weaver_test()
@pytest.mark.parametrize(
    "kind,object_type,identity",
    [
        ("Warehouse", "table", "Warehouse/Model/Sales.Customer"),
        ("Lakehouse", "table", "Lakehouse/Input/Tables/Sales.Customer"),
        ("Lakehouse", "folder", "Lakehouse/Input/Files/Raw.Export"),
        ("Lakehouse", "view", "Lakehouse/Input/Tables/Sales.Active"),
    ],
)
def test_mirror_rebind_publishes_destination_guids_without_changing_source(
    tmp_path, kind, object_type, identity
):
    from weaver import __version__

    item_name = "Model" if kind == "Warehouse" else "Input"
    relation = Borrowed(
        WeaverDocumentId.parse(identity),
        object_type,
        "view" if kind == "Warehouse" else object_type,
    )
    resolved = _resolved(item_name, kind, relations=(relation,))
    session, client = recorded_session(resolved, tmp_path)
    resolver = session.resolver()
    source = resolver.resolve(ItemRef(item_name), item_type=kind)
    destination = resolver.resolve(ItemRef(item_name + "_Dev"), item_type=kind)
    source_row = resolved.installations[resolved.items[0].item]
    source_row.update(
        workspace_name="Analytics",
        workspace_id=WORKSPACE_ID,
        item_id=source.id,
        weaver_version="source-version",
    )
    original = deepcopy(resolved.installations)
    source_files = {}
    if kind == "Lakehouse":
        root = resolver.external_root(source)
        if object_type in ("folder", "table"):
            path = tmp_path / root.value.removeprefix(tmp_path.as_posix() + "/")
            area = "Files" if object_type == "folder" else "Tables"
            (path / area / relation.schema / relation.name).mkdir(parents=True)
        load = root.join("Files", "_", "Load", "module.py")
        session.store().write(load, b"source runtime bytes")
        source_files[load.value] = b"source runtime bytes"

    plan, payloads, _ = mirror_mutation_plan(resolved, session=session)
    final = "\n".join(
        line.removeprefix("    ")
        for line in payloads[_actions(plan)[PUBLISH].payload].decode().splitlines()
    )
    expected = {
        **source_row,
        "target_name": destination.name,
        "workspace_name": resolver.workspace.name,
        "workspace_id": destination.workspace_id,
        "item_id": destination.id,
        "weaver_version": __version__,
    }
    assert (
        render_merge(INSTALLATION, [expected], scope=InstallationScope(kind, item_name))
        in final
    )
    assert source.id not in final
    assert resolved.installations == original
    for path, content in source_files.items():
        from weaver.locations import Location

        assert session.store().read(Location(path)) == content
    targets = {target.id: target for target in plan.targets}
    for _, _, action in plan.actions():
        if action.target_id is not None:
            target = targets[action.target_id]
            physical = resolver.resolve(
                ItemRef(target.name), item_type=target.kind.title()
            )
            assert physical.id != source.id
    assert client.requested == ["workspaces", f"workspaces/{WORKSPACE_ID}/items"]


@weaver_test()
@pytest.mark.parametrize(
    "name,field",
    [("Weaver_Dev", "id"), ("Model_Dev", "id"), ("Model_Dev", "workspace_id")],
)
@pytest.mark.parametrize(
    "invalid", [None, "", "Model_Dev", " 465bc1d2-4900-4644-b99c-e706f5f0fff2"]
)
def test_mirror_refuses_invalid_resolved_physical_guids(tmp_path, name, field, invalid):
    from weaver.errors import BuildError

    resolved = _resolved("Model", "Warehouse")
    session, _ = recorded_session(resolved, tmp_path)
    resolver = session.resolver()
    key = name + ":Warehouse"
    resolver._items[key] = replace(resolver._items[key], **{field: invalid})
    with pytest.raises(BuildError, match="invalid .*GUID"):
        mirror_mutation_plan(resolved, session=session)
