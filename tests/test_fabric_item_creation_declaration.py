"""What Weaver asks Fabric for when it creates an item.

A live workspace is the only place a wrong creation payload shows up — and it
shows up late, because a Lakehouse without schemas looks entirely normal until
something needs a schema in it. Fabric settles this at creation and offers no way
to change it afterwards, so the item has to be deleted and made again.

Hence a fake client here: the request Weaver *composes* is checkable in CI, with
no workspace, no credential and no capacity.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from weaver.errors import CommandError
from weaver.fabric.resources import LAKEHOUSE, WorkspaceItem, create_lakehouse

WORKSPACE = WorkspaceItem(id="workspace-id", name="Analytics")


class _CreateClient:
    """A workspace holding nothing, which answers a create immediately."""

    def __init__(self, status_code: int = 201):
        self.requests = []
        self.status_code = status_code

    def paged(self, path):
        return []

    def get_json(self, path):
        return {"value": []}

    def request(self, method, path, *, payload, expected):
        self.requests.append((method, path, payload, expected))
        return SimpleNamespace(
            status_code=self.status_code,
            json=lambda: {"id": "lakehouse-id", "displayName": payload["displayName"]},
            text="",
            content=b"{}",
            headers={},
        )


def test_a_lakehouse_is_created_with_schemas_enabled():
    """The catalogue lives in a schema called ``_``; without this it cannot exist.

    Fabric only accepts ``enableSchemas`` at creation, so omitting it produced a
    Lakehouse that had to be deleted and remade by hand.
    """

    client = _CreateClient()

    item = create_lakehouse(WORKSPACE, "PYTEST_WEAVER", client=client)

    assert client.requests == [
        (
            "POST",
            "workspaces/workspace-id/lakehouses",
            {
                "displayName": "PYTEST_WEAVER",
                "creationPayload": {"enableSchemas": True},
            },
            (200, 201, 202, 409),
        )
    ]
    assert (item.id, item.name, item.type) == (
        "lakehouse-id",
        "PYTEST_WEAVER",
        LAKEHOUSE,
    )


def test_a_name_fabric_has_not_released_yet_reports_why():
    """A deleted item's name is held for some minutes, and 409 is the symptom."""

    class Held(_CreateClient):
        def request(self, method, path, *, payload, expected):
            super().request(method, path, payload=payload, expected=expected)
            return SimpleNamespace(
                status_code=409,
                json=lambda: {"message": "ItemDisplayNameNotAvailableYet"},
                text="",
                content=b"{}",
                headers={},
            )

    with pytest.raises(CommandError, match="ItemDisplayNameNotAvailableYet"):
        create_lakehouse(WORKSPACE, "PYTEST_WEAVER", client=Held())
