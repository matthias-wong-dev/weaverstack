"""The Fabric requirement set has one source in project metadata."""

from support.weaver_test import weaver_test

from weaver.fabric.environment import project_root, runtime_dependencies
from weaver.fabric.environment_packages import DESKTOP_ONLY, runtime_requirements


@weaver_test()
def test_fabric_requirements_come_from_pyproject():
    assert tuple(runtime_dependencies()) == runtime_requirements(project_root())


@weaver_test()
def test_fabric_requirements_include_hosted_runtime_packages():
    names = {
        requirement.split("=", 1)[0].lower() for requirement in runtime_dependencies()
    }
    assert {"pyyaml", "sqlparse", "mssql-python"} <= names


@weaver_test()
def test_desktop_packages_are_excluded_from_fabric_requirements():
    names = {
        requirement.split("=", 1)[0].lower() for requirement in runtime_dependencies()
    }
    assert not names & DESKTOP_ONLY


@weaver_test()
def test_local_environment_definition_is_retired():
    assert not (project_root() / "deployment/fabric/environment.yml").exists()
