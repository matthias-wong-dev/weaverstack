"""The Fabric Environment definition must cover Weaver's runtime dependencies.

``pyproject.toml`` is authoritative for what installed Weaver needs;
``deployment/fabric/environment.yml`` is what Fabric actually installs. If the
two drift, a package Weaver imports would simply be missing in a published
Environment. This test makes that drift a failing build rather than a runtime
``ModuleNotFoundError`` inside a Spark session.
"""

from __future__ import annotations

from support.weaver_test import weaver_test

from weaver.fabric.environment import (
    environment_dependencies,
    missing_from_environment,
    runtime_dependencies,
)


@weaver_test()
def test_every_runtime_dependency_is_installed_by_the_environment():
    assert missing_from_environment() == []


@weaver_test()
def test_the_environment_installs_the_warehouse_sql_driver():
    # mssql-python is the one runtime dependency easy to forget, because nothing
    # local needs it, Warehouse SQL only runs in Fabric.
    staged = {name.lower() for name in environment_dependencies()}
    assert "mssql-python" in staged


@weaver_test()
def test_runtime_dependencies_are_declared():
    # A guard on the guard: if the parse returned nothing, the drift check above
    # would pass vacuously.
    assert runtime_dependencies()


@weaver_test()
def test_no_desktop_package_is_installed_into_the_environment():
    """The exclusion is a decision, so it is checked in both directions.

    A package named desktop-only must actually be absent from the Environment;
    otherwise the exclusion list is describing something that is not true.
    """

    from weaver.fabric.environment import DESKTOP_ONLY

    staged = {name.lower() for name in environment_dependencies()}
    assert not staged & {name.lower() for name in DESKTOP_ONLY}
