"""What ``weaver mirror`` decides before it reaches a workspace.

The address it forks from, the fork it refuses, and the order the destination is
rebuilt in. Everything here is settled without a tenant, which is the point: a
fork empties a Warehouse, so what it refuses it must refuse first.
"""

from __future__ import annotations

import pytest
from support.weaver_test import weaver_test

import weaver
from weaver.config import parse_workspace
from weaver.errors import CommandError, ConfigError
from weaver.operations.mirror import MirrorResult
from weaver.workspaces import CatalogueRef, Workspace


def _workspace(**overrides) -> Workspace:
    values = {
        "workspace": "Analytics",
        "catalogue": "Warehouse/Weaver_Dev",
        "mirror": "Warehouse/Weaver",
    }
    values.update(overrides)
    return Workspace(**values)


# --- the source address -------------------------------------------------------


@weaver_test()
def test_configuration_names_one_source_catalogue():
    workspace = parse_workspace(
        {
            "workspace": "Analytics",
            "catalogue": "Warehouse/Weaver_Dev",
            "mirror": "Warehouse/Weaver",
        }
    )

    assert workspace.mirror == CatalogueRef(workspace=None, name="Weaver")
    assert str(workspace.mirror) == "Warehouse/Weaver"


@weaver_test()
def test_a_source_may_name_the_workspace_holding_it():
    """The form parses, so an address written today keeps its meaning."""

    workspace = parse_workspace(
        {
            "workspace": "Analytics",
            "catalogue": "Warehouse/Weaver_Dev",
            "mirror": "Production/Warehouse/Weaver",
        }
    )

    assert workspace.mirror == CatalogueRef(workspace="Production", name="Weaver")
    assert not workspace.mirror.is_local_to("Analytics")


@weaver_test()
def test_a_workspace_forks_nothing_unless_it_says_so():
    assert parse_workspace({"workspace": "Analytics"}).mirror is None


@weaver_test()
@pytest.mark.parametrize(
    "written",
    ["Weaver", "Lakehouse/Weaver", "A/B/C/D", "Analytics/Lakehouse/Weaver"],
)
def test_a_source_that_is_not_a_warehouse_catalogue_is_refused(written):
    with pytest.raises(ConfigError):
        parse_workspace(
            {"workspace": "Analytics", "catalogue": "Warehouse/W", "mirror": written}
        )


@weaver_test()
def test_the_destination_is_this_workspaces_own_catalogue():
    assert str(_workspace().catalogue_ref) == "Analytics/Warehouse/Weaver_Dev"


# --- what is refused, and refused before anything is emptied ------------------


@weaver_test()
def test_a_workspace_naming_no_source_says_what_to_set(monkeypatch):
    _given(monkeypatch, _workspace(mirror=None))

    with pytest.raises(CommandError, match="mirror:"):
        weaver.mirror(no_item=True, session=_never())


@weaver_test()
def test_a_source_in_another_workspace_is_refused(monkeypatch):
    """A Fabric Warehouse reaches its own workspace and no further.

    Refused rather than attempted, because the step after this empties the
    destination Warehouse.
    """

    _given(monkeypatch, _workspace(mirror="Production/Warehouse/Weaver"))

    with pytest.raises(CommandError, match="must be in the workspace"):
        weaver.mirror(no_item=True, session=_never())


@weaver_test()
def test_forking_a_catalogue_into_itself_is_refused(monkeypatch):
    """The destination is emptied first, so this would destroy the source."""

    _given(monkeypatch, _workspace(catalogue="Warehouse/Weaver"))

    with pytest.raises(CommandError, match="would empty the catalogue it copies"):
        weaver.mirror(no_item=True, session=_never())


@weaver_test()
def test_naming_items_and_no_item_together_is_refused(monkeypatch):
    _given(monkeypatch, _workspace())

    with pytest.raises(CommandError, match="not both"):
        weaver.mirror(["Warehouse/Model"], no_item=True, session=_never())


@weaver_test()
def test_selecting_an_item_says_the_rebinding_is_not_here_yet(monkeypatch):
    """Silence would leave the item where the copied catalogue put it."""

    _given(monkeypatch, _workspace())

    with pytest.raises(CommandError, match="rebinds no physical item yet"):
        weaver.mirror(["Warehouse/Model"], session=_never())


@weaver_test()
def test_a_configured_target_is_selected_when_no_item_is_named(monkeypatch):
    """Bare ``mirror`` means every configured target, as ``build`` does."""

    from weaver.declaration.model import WeaverItemId
    from weaver.workspaces import TargetDeclaration

    _given(
        monkeypatch,
        _workspace(
            targets={
                WeaverItemId.parse("Warehouse/Model"): TargetDeclaration("Model_Dev")
            }
        ),
    )

    with pytest.raises(CommandError, match="Warehouse/Model"):
        weaver.mirror(session=_never())


# --- what it reports ----------------------------------------------------------


@weaver_test()
def test_a_result_says_what_moved_and_what_did_not():
    result = MirrorResult(
        workspace="Analytics",
        source_catalogue="Warehouse/Weaver",
        destination_catalogue="Analytics/Warehouse/Weaver_Dev",
        wiped=("Warehouse/Weaver_Dev",),
        copied={"Registry": 12, "Installation": 2},
        uncopied=("Log", "LoadStatistic"),
    )

    assert result.rows == 14
    assert result.to_mapping()["wiped"] == ["Warehouse/Weaver_Dev"]
    assert result.to_mapping()["uncopied"] == ["Log", "LoadStatistic"]


# --- helpers ------------------------------------------------------------------


class _Never:
    """A session that fails if a refusal ever reaches a workspace."""

    def __getattr__(self, name):
        raise AssertionError(f"mirror reached the workspace: {name}")


def _never():
    return _Never()


def _given(monkeypatch, workspace: Workspace) -> None:
    """Resolve to this Workspace, whatever the working directory holds."""

    import weaver.operations.workspace as module

    monkeypatch.setattr(module, "_operation_workspace", lambda **_kwargs: workspace)
