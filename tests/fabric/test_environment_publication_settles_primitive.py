"""Publication installs a runtime, and the next publication is a verified no-op.

Run it with ``WEAVER_PYTEST_INJECT_WEAVER=0``, so what the session imports is
what the Environment published:

.. code-block:: bash

    WEAVER_PYTEST_INJECT_WEAVER=0 pytest -m "fabric and hosted" -k publication_settles

``dev=True``, as in the preservation primitive: the suite's Environment carries
a Weaver wheel built from a checkout, and released mode would remove it.

The session is opened after the publish and closed before the second one. A
session already running was served an earlier image, so it is evidence about
that image rather than about this publication.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from support.weaver_test import weaver_test

CHECKOUT = Path(__file__).resolve().parents[2]


@weaver_test(hosted=True)
@pytest.mark.slow
def test_publication_installs_a_runtime_and_settles_to_a_noop(
    exclusive_livy_slot,
    fabric_workspace,
    fabric_client,
    fabric_target_lakehouse,
    injected_weaver_bootstrap,
):
    """Publish, run Weaver in a session started afterwards, publish again.

    The second publication is the claim: ``AlreadyInstalled`` over an
    Environment whose published libraries a cold session has just imported.
    """

    if injected_weaver_bootstrap is not None:
        pytest.skip(
            "the suite staged this checkout's wheel; run with "
            "WEAVER_PYTEST_INJECT_WEAVER=0 to exercise publication"
        )

    from weaver.fabric import LivySession, emit_source
    from weaver.fabric.environment import (
        ENVIRONMENT,
        find_workspace,
        library_wheels,
        publish_environment,
        publishes_weaver,
        read_published,
    )
    from weaver.fabric.resources import find_item

    first = publish_environment(
        fabric_workspace.workspace,
        fabric_workspace.environment,
        dev=True,
        client=fabric_client,
        root=CHECKOUT,
    )
    assert first.wheel_filename, "a development publication supplies a wheel"

    environment = find_item(
        find_workspace(fabric_workspace.workspace, client=fabric_client),
        fabric_workspace.environment.name,
        item_type=ENVIRONMENT,
        client=fabric_client,
    )
    published = read_published(environment, client=fabric_client)
    assert publishes_weaver(
        published, wheel=first.wheel_filename, requirement=first.weaver_requirement
    ), (
        f"publication reported {first.publish_status!r}, and the Environment "
        f"has published {library_wheels(published)}"
    )

    session = LivySession.for_workspace(
        fabric_workspace,
        bootstrap=emit_source(),
        lakehouse=fabric_target_lakehouse.name,
    )
    session.start()
    try:
        observed = session.run(
            "from importlib.metadata import version\n"
            "from weaver.resolution import resolver_for\n"
            "from weaver.targets import ItemRef\n"
            "from weaver.workspaces import Workspace\n"
            f"workspace = Workspace(workspace={fabric_workspace.workspace!r}, "
            f"catalogue={fabric_workspace.catalogue!r}, "
            f"environment={str(fabric_workspace.environment)!r})\n"
            "root = resolver_for(workspace).lakehouse("
            f"ItemRef({fabric_target_lakehouse.name!r})).value\n"
            "emit({'dist': version('weaverstack'), 'root': root, "
            "'rows': spark.range(3).count()})\n"
        ).payload
    finally:
        session.close()

    # Real work through the package the Environment supplied: Weaver imported,
    # resolved a Lakehouse over notebookutils, and Spark ran.
    assert observed["dist"] in first.wheel_filename
    assert observed["root"]
    assert observed["rows"] == 3

    second = publish_environment(
        fabric_workspace.workspace,
        fabric_workspace.environment,
        dev=True,
        client=fabric_client,
        root=CHECKOUT,
    )

    assert second.publish_status == "AlreadyInstalled"
    assert second.published is False
    assert second.action == "unchanged"
    assert second.wheel_filename == first.wheel_filename
