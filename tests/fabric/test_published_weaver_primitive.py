"""The Environment-published wheel, imported and executed inside Fabric.

Release mode only. The ordinary hosted run stages this checkout's wheel and puts
it first on the session's ``sys.path``, and the bootstrap in
``tests/fabric/injected_weaver.py`` reads ``weaver.__file__`` and fails the
session unless the package came from the extraction directory. So every hosted
test already proves that Weaver imports in Fabric, resolves over
``notebookutils``, opens TDS on the session identity, reaches the session's Spark
catalogue and writes through the session-native store. Each of those had a probe
of its own here, and the estate work makes the same five claims.

What the injected checkout cannot speak for is ``weaver fabric environment
publish``. Run this with ``WEAVER_PYTEST_INJECT_WEAVER=0`` before a release:

.. code-block:: bash

    WEAVER_PYTEST_INJECT_WEAVER=0 pytest -m "fabric and hosted"

It skips otherwise, because in the injected mode the Environment's published set
says nothing about what the session is running.
"""

from __future__ import annotations

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest
from support.weaver_test import weaver_test


def _checkout_version() -> str:
    """Compute the checkout version from Hatch's root-only version source."""

    source = Path(__file__).resolve().parents[2] / "hatch_build.py"
    spec = spec_from_file_location("weaver_hatch_build", source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load Weaver's version source from {source}")
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.compute_version()


def _wheel_version(filename: str) -> str:
    """``weaverstack-0.1.2.dev123456-py3-none-any.whl`` -> the version."""

    return filename.split("-")[1]


@weaver_test(hosted=True)
def test_the_published_wheel_imports_and_runs_in_the_session(
    livy_session, fabric_workspace, fabric_target_lakehouse, injected_weaver_bootstrap
):
    """One release-mode smoke test: the published package imports and works.

    Two questions about the Environment. The checkout's version must be among
    its published wheels, and the session must be running one of them. The
    second is not immediate: Fabric reports a successful publish while new
    sessions still start on the previous image. The answer there is to wait, not
    to publish again.

    Then one piece of real work through the package the Environment supplied, so
    a wheel that imports and cannot resolve a Lakehouse fails here.
    """

    if injected_weaver_bootstrap is not None:
        pytest.skip(
            "the suite staged this checkout's wheel; run with "
            "WEAVER_PYTEST_INJECT_WEAVER=0 for the published wheel"
        )

    from weaver.fabric.client import FabricClient
    from weaver.fabric.environment import (
        ENVIRONMENT,
        find_workspace,
        library_wheels,
        read_published,
    )
    from weaver.fabric.resources import find_item

    payload = livy_session.run(
        "from importlib.metadata import version\n"
        "from weaver.workspaces import Workspace\n"
        "from weaver.targets import ItemRef\n"
        "from weaver.resolution import resolver_for\n"
        f"workspace = Workspace(workspace={fabric_workspace.workspace!r}, "
        f"catalogue={fabric_workspace.catalogue!r}, "
        f"environment={fabric_workspace.environment!r})\n"
        "resolver = resolver_for(workspace)\n"
        f"root = resolver.lakehouse(ItemRef({fabric_target_lakehouse.name!r})).value\n"
        "emit({'dist': version('weaverstack'), 'root': root})\n",
    ).payload

    client = FabricClient()
    environment = find_item(
        find_workspace(fabric_workspace.workspace, client=client),
        fabric_workspace.environment,
        item_type=ENVIRONMENT,
        client=client,
    )
    published = {
        _wheel_version(name)
        for name in library_wheels(read_published(environment, client=client))
    }
    wanted = _checkout_version()

    assert wanted in published, (
        f"this checkout is weaverstack {wanted}, but the Environment has "
        f"published {sorted(published)}. Run `weaver fabric environment publish`"
    )
    assert payload["dist"] in published, (
        f"this Fabric session is running weaverstack {payload['dist']}, but the "
        f"Environment has published {sorted(published)}. A publish reports "
        "Success before new sessions are served the new image, wait for it "
        "rather than publishing again, and only rerun "
        "`weaver fabric environment publish` if the published set is not this "
        "checkout's."
    )
    # And it did real work: the session resolver reached the Lakehouse.
    assert payload["root"]
