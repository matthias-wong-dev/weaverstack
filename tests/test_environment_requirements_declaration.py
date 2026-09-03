"""The Fabric requirement set has one source in project metadata."""

from support.weaver_test import weaver_test

from weaver.fabric.environment import project_root, runtime_dependencies
from weaver.fabric.environment_definition import DESKTOP_ONLY, runtime_requirements


def _names() -> set[str]:
    """The distribution each requirement names, whatever version it pins.

    Parsed rather than split on a character: a requirement may carry a specifier,
    and ``sqlparse>=0.6.0`` split on ``=`` names ``sqlparse>``.
    """

    from packaging.requirements import Requirement

    return {Requirement(text).name.lower() for text in runtime_dependencies()}


@weaver_test()
def test_fabric_requirements_come_from_pyproject():
    assert tuple(runtime_dependencies()) == runtime_requirements(project_root())


@weaver_test()
def test_fabric_requirements_include_hosted_runtime_packages():
    assert {"pyyaml", "sqlparse", "mssql-python"} <= _names()


@weaver_test()
def test_desktop_packages_are_excluded_from_fabric_requirements():
    assert not _names() & DESKTOP_ONLY


@weaver_test()
def test_local_environment_definition_is_retired():
    assert not (project_root() / "deployment/fabric/environment.yml").exists()
