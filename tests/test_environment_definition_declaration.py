"""The Fabric Environment definition, and the Weaver overlay over one."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric.environment_definition import (
    CUSTOM_LIBRARIES,
    DESKTOP_ONLY,
    EXTERNAL_LIBRARIES,
    PLATFORM,
    SPARK_COMPUTE,
    EnvironmentDefinition,
    definition_payload,
    development_external_libraries,
    environment_name_from_path,
    normalise_distribution,
    pip_entries,
    read_environment_definition,
    released_external_libraries,
    runtime_requirements,
    weaver_requirement,
)

PLATFORM_JSON = '{"metadata": {"type": "Environment", "displayName": "Runtime"}}'
SPARK_YML = "runtime_version: 1.3\ndriver_cores: 8\n"


def _definition(tmp_path: Path, name: str = "Runtime.Environment", **parts) -> Path:
    """One local Environment definition directory holding the given parts."""

    root = tmp_path / name
    for relative, content in parts.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(
            content if isinstance(content, bytes) else content.encode("utf-8")
        )
    root.mkdir(parents=True, exist_ok=True)
    return root


# --- the directory -------------------------------------------------------------


@weaver_test()
def test_the_directory_names_the_environment(tmp_path):
    """``Something.Environment`` publishes ``Something``."""

    assert environment_name_from_path(_definition(tmp_path, "Sales.Environment")) == (
        "Sales"
    )


@weaver_test()
def test_a_definition_may_live_anywhere(tmp_path):
    """``Environment/`` is a repository convention, not a rule."""

    nested = tmp_path / "deploy" / "fabric"
    nested.mkdir(parents=True)
    root = _definition(nested, "Runtime.Environment", **{PLATFORM: PLATFORM_JSON})

    assert environment_name_from_path(root) == "Runtime"
    assert read_environment_definition(root).parts[PLATFORM] == PLATFORM_JSON.encode()


@pytest.mark.parametrize("name", ["Runtime", "Runtime.environment", ".Environment"])
@weaver_test()
def test_a_directory_that_is_not_an_environment_is_refused(tmp_path, name):
    with pytest.raises(CommandError, match="<Name>.Environment"):
        environment_name_from_path(tmp_path / name)


@weaver_test()
def test_a_missing_definition_is_refused(tmp_path):
    with pytest.raises(CommandError, match="no such Environment definition"):
        read_environment_definition(tmp_path / "Runtime.Environment")


@weaver_test()
def test_a_file_is_not_a_definition(tmp_path):
    path = tmp_path / "Runtime.Environment"
    path.write_text("not a directory", encoding="utf-8")

    with pytest.raises(CommandError, match="is a directory, not a file"):
        read_environment_definition(path)


@weaver_test()
def test_an_unsupported_part_is_refused(tmp_path):
    """Fabric takes four kinds of part, so a fifth is reported here.

    The message names the file the user wrote, and the read happens before
    anything is sent.
    """

    root = _definition(tmp_path, **{"README.md": "notes"})

    with pytest.raises(CommandError, match="is not an Environment definition part"):
        read_environment_definition(root)


@weaver_test()
def test_malformed_external_libraries_are_refused_at_read(tmp_path):
    root = _definition(tmp_path, **{EXTERNAL_LIBRARIES: "dependencies: [unclosed\n"})

    with pytest.raises(CommandError, match="not valid YAML"):
        read_environment_definition(root)


@weaver_test()
def test_every_part_is_carried_as_inline_base64(tmp_path):
    """The transport Fabric's definition APIs take."""

    root = _definition(
        tmp_path,
        **{
            PLATFORM: PLATFORM_JSON,
            SPARK_COMPUTE: SPARK_YML,
            f"{CUSTOM_LIBRARIES}other-1.0-py3-none-any.whl": b"\x00binary\xff",
        },
    )

    payload = definition_payload(read_environment_definition(root))
    by_path = {part["path"]: part for part in payload["parts"]}

    assert set(by_path) == {
        PLATFORM,
        SPARK_COMPUTE,
        f"{CUSTOM_LIBRARIES}other-1.0-py3-none-any.whl",
    }
    assert all(part["payloadType"] == "InlineBase64" for part in payload["parts"])
    assert base64.b64decode(by_path[SPARK_COMPUTE]["payload"]) == SPARK_YML.encode()


# --- the external library list -------------------------------------------------


@weaver_test()
def test_a_list_that_does_not_exist_becomes_one_weaver_requirement(tmp_path):
    assert pip_entries(released_external_libraries("", source="x"), source="x") == (
        "weaverstack",
    )


@weaver_test()
def test_an_unrelated_pip_list_is_preserved(tmp_path):
    text = "dependencies:\n  - pip:\n      - fuzzywuzzy==0.18.0\n      - numpy\n"

    entries = pip_entries(released_external_libraries(text, source="x"), source="x")

    assert entries == ("fuzzywuzzy==0.18.0", "numpy", "weaverstack")


@pytest.mark.parametrize(
    "written", ["weaverstack==0.4.0", "weaverstack>=0.4", "weaverstack"]
)
@weaver_test()
def test_an_authored_weaver_specifier_is_kept(written):
    """The file's spelling wins, so a pinned requirement is not widened."""

    text = f"dependencies:\n  - pip:\n      - {written}\n"

    result = released_external_libraries(text, source="x")

    assert pip_entries(result, source="x") == (written,)


@weaver_test()
def test_a_weaver_requirement_is_never_added_twice():
    text = "dependencies:\n  - pip:\n      - WeaverStack>=0.4\n"

    entries = pip_entries(released_external_libraries(text, source="x"), source="x")

    # PEP 503 compares distribution names case-insensitively, so this entry is
    # the Weaver requirement.
    assert entries == ("WeaverStack>=0.4",)


@weaver_test()
def test_development_removes_every_pypi_weaver_requirement():
    text = "dependencies:\n  - pip:\n      - weaverstack==0.4.0\n      - numpy\n"

    entries = pip_entries(
        development_external_libraries(text, requirements=(), source="x"), source="x"
    )

    assert entries == ("numpy",)


@weaver_test()
def test_development_names_weavers_own_requirements():
    """A Fabric custom wheel installs no dependencies, so they are named here."""

    entries = pip_entries(
        development_external_libraries(
            "", requirements=("pyyaml", "sqlparse>=0.6.0"), source="x"
        ),
        source="x",
    )

    assert entries == ("pyyaml", "sqlparse>=0.6.0")


@weaver_test()
def test_a_requirement_already_listed_keeps_its_authored_spelling():
    text = "dependencies:\n  - pip:\n      - sqlparse==0.6.1\n"

    entries = pip_entries(
        development_external_libraries(
            text, requirements=("sqlparse>=0.6.0",), source="x"
        ),
        source="x",
    )

    assert entries == ("sqlparse==0.6.1",)


@weaver_test()
def test_comments_and_pip_options_survive_the_overlay():
    """Line work rather than a YAML round trip, and this is why.

    A ``--index-url`` sits in the same list as the requirements, and the comment
    above it is the reason somebody put it there.
    """

    text = (
        "# The packages this Environment installs.\n"
        "dependencies:\n"
        "  - matplotlib==1.0\n"
        "  - pip:\n"
        "      - --index-url https://example.invalid/simple\n"
        "      - fuzzywuzzy==0.18.0\n"
    )

    result = released_external_libraries(text, source="x")

    assert "# The packages this Environment installs." in result
    assert "  - matplotlib==1.0" in result
    assert "      - --index-url https://example.invalid/simple" in result
    assert weaver_requirement(pip_entries(result, source="x")) == "weaverstack"


@weaver_test()
def test_a_dependencies_block_that_is_not_a_list_is_refused():
    with pytest.raises(CommandError, match="dependencies that are not a list"):
        pip_entries("dependencies: numpy\n", source="x")


# --- what Weaver needs inside Fabric -------------------------------------------


@weaver_test()
def test_the_fabric_requirements_leave_the_desktop_behind(tmp_path):
    """Publishing Weaver into Fabric must not drag the transports along."""

    (tmp_path / "pyproject.toml").write_text(
        '[project]\ndependencies = ["pyyaml", "requests>=2.31", "mssql-python"]\n',
        encoding="utf-8",
    )

    assert runtime_requirements(tmp_path) == ("pyyaml", "mssql-python")
    assert normalise_distribution("Requests") in DESKTOP_ONLY


@weaver_test()
def test_custom_libraries_are_listed_by_filename():
    definition = EnvironmentDefinition(
        parts={
            PLATFORM: b"{}",
            f"{CUSTOM_LIBRARIES}weaverstack-0.1.0-py3-none-any.whl": b"w",
            f"{CUSTOM_LIBRARIES}other-1.0-py3-none-any.whl": b"o",
        }
    )

    assert definition.custom_libraries() == (
        "other-1.0-py3-none-any.whl",
        "weaverstack-0.1.0-py3-none-any.whl",
    )


# --- one effective Weaver requirement -----------------------------------------


@weaver_test()
def test_duplicate_weaver_requirements_collapse_to_the_authored_spelling():
    """Two entries for one distribution leave which version installs to pip."""

    text = (
        "dependencies:\n"
        "  - pip:\n"
        "      - weaverstack==0.4.0\n"
        "      - numpy\n"
        "      - weaverstack>=0.2\n"
    )

    entries = pip_entries(released_external_libraries(text, source="x"), source="x")

    assert entries == ("weaverstack==0.4.0", "numpy")


@weaver_test()
def test_case_variant_weaver_requirements_are_the_same_requirement():
    text = (
        "dependencies:\n"
        "  - pip:\n"
        "      - weaverstack\n"
        "      - WeaverStack==1.0\n"
        "      - weaverstack>=2\n"
    )

    entries = pip_entries(released_external_libraries(text, source="x"), source="x")

    assert entries == ("weaverstack",)


# --- what a development publication may keep ----------------------------------


WEAVER_NEEDS = ("pyyaml", "sqlparse>=0.6.0", "mssql-python")


@pytest.mark.parametrize(
    "written", ["sqlparse==0.5.3", "sqlparse<0.6", "sqlparse<=0.5"]
)
@weaver_test()
def test_a_requirement_weaver_cannot_run_against_is_refused(written):
    """A published Environment Weaver cannot import from is caught here.

    The alternative is an ImportError in the first notebook cell, minutes after
    the publish.
    """

    text = f"dependencies:\n  - pip:\n      - {written}\n"

    with pytest.raises(CommandError, match="Weaver cannot run against") as raised:
        development_external_libraries(text, requirements=WEAVER_NEEDS, source="x")

    message = str(raised.value)
    assert written in message
    assert "sqlparse>=0.6.0" in message
    assert "No Environment changes were staged." in message


@pytest.mark.parametrize(
    "written", ["sqlparse==0.6.1", "sqlparse>=0.5", "sqlparse", "sqlparse>=0.6,<1.0"]
)
@weaver_test()
def test_a_compatible_requirement_keeps_its_authored_specifier(written):
    """Weaver names a floor, and anything that can meet it stays as it is."""

    text = f"dependencies:\n  - pip:\n      - {written}\n"

    entries = pip_entries(
        development_external_libraries(text, requirements=WEAVER_NEEDS, source="x"),
        source="x",
    )

    assert written in entries


@weaver_test()
def test_every_conflicting_requirement_is_reported_at_once():
    """One publish, one list of what to change."""

    text = "dependencies:\n  - pip:\n      - sqlparse==0.5.3\n      - pyyaml==1.0\n"

    with pytest.raises(CommandError) as raised:
        development_external_libraries(
            text, requirements=("pyyaml>=6", "sqlparse>=0.6.0"), source="x"
        )

    message = str(raised.value)
    assert "sqlparse==0.5.3" in message
    assert "pyyaml==1.0" in message
