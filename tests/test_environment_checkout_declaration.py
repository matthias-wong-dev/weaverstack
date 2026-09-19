"""``--dev`` resolves a Weaver source checkout, or none at all.

A development publication builds a wheel from the checkout the installed
package came from. The ancestor holding the nearest ``pyproject.toml`` is not
that checkout when Weaver was installed from a wheel into a virtual environment
nested under another project, so a candidate is identified before it is built.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from support.weaver_test import weaver_test

from weaver.errors import CommandError
from weaver.fabric import environment as env_mod
from weaver.fabric.environment import names_weaver, project_root

WEAVER_PROJECT = '[project]\nname = "weaverstack"\nversion = "0.1.0"\n'
OTHER_PROJECT = '[project]\nname = "parcel-analytics"\nversion = "1.0.0"\n'

NO_CHECKOUT = "no weaverstack source checkout"

#: Where a wheel installation puts the module, relative to the environment.
SITE_PACKAGES = "Lib/site-packages/weaver/fabric/environment.py"


def _project(directory: Path, text: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pyproject.toml").write_text(text, encoding="utf-8")
    return directory


def _editable(checkout: Path) -> Path:
    return checkout / "src/weaver/fabric/environment.py"


def _installed(environment: Path) -> Path:
    return environment / SITE_PACKAGES


# --- identifying a candidate ----------------------------------------------------


@weaver_test()
def test_a_weaver_pyproject_is_identified(tmp_path):
    checkout = _project(tmp_path, WEAVER_PROJECT)

    assert names_weaver(checkout / "pyproject.toml")


@weaver_test()
def test_another_projects_pyproject_is_not(tmp_path):
    other = _project(tmp_path, OTHER_PROJECT)

    assert not names_weaver(other / "pyproject.toml")


@weaver_test()
def test_the_distribution_name_is_compared_the_way_pep_503_compares_it(tmp_path):
    checkout = _project(tmp_path, '[project]\nname = "WeaverStack"\n')

    assert names_weaver(checkout / "pyproject.toml")


@weaver_test()
def test_metadata_that_cannot_be_read_names_nothing(tmp_path):
    broken = _project(tmp_path / "broken", "[project\nname =")
    nameless = _project(tmp_path / "nameless", "[build-system]\nrequires = []\n")

    assert not names_weaver(broken / "pyproject.toml")
    assert not names_weaver(nameless / "pyproject.toml")
    assert not names_weaver(tmp_path / "absent" / "pyproject.toml")


# --- resolving the checkout -----------------------------------------------------


@weaver_test()
def test_an_editable_checkout_resolves_to_its_root(tmp_path):
    checkout = _project(tmp_path / "weaverstack", WEAVER_PROJECT)

    assert project_root(_editable(checkout)) == checkout


@weaver_test()
def test_a_wheel_installation_with_no_checkout_is_refused(tmp_path):
    module = _installed(tmp_path / "venv")

    with pytest.raises(CommandError, match=NO_CHECKOUT):
        project_root(module)


@weaver_test()
def test_a_venv_nested_under_another_project_does_not_resolve_to_it(tmp_path):
    """The reported failure: the nearest ancestor project is not Weaver."""

    unrelated = _project(tmp_path / "parcel", OTHER_PROJECT)

    with pytest.raises(CommandError, match=NO_CHECKOUT):
        project_root(_installed(unrelated / ".venv"))


@weaver_test()
def test_metadata_that_cannot_be_read_is_skipped_rather_than_built(tmp_path):
    """A malformed or nameless candidate fails the identity check, not the walk."""

    checkout = _project(tmp_path / "weaverstack", WEAVER_PROJECT)
    _project(checkout / "examples" / "broken", "[project\nname =")
    nameless = _project(checkout / "examples" / "broken" / "nameless", "[project]\n")

    assert project_root(_installed(nameless / ".venv")) == checkout


@weaver_test()
def test_the_working_directory_is_never_the_answer(tmp_path, monkeypatch):
    checkout = _project(tmp_path / "weaverstack", WEAVER_PROJECT)
    elsewhere = _project(tmp_path / "elsewhere", OTHER_PROJECT)
    monkeypatch.chdir(elsewhere)

    with pytest.raises(CommandError, match=NO_CHECKOUT):
        project_root(_installed(elsewhere / ".venv"))
    assert project_root(_editable(checkout)) == checkout


@weaver_test()
def test_this_installation_resolves_to_this_checkout():
    assert (project_root() / "VERSION").is_file()
    assert names_weaver(project_root() / "pyproject.toml")


# --- nothing unidentified reaches a build backend -------------------------------


class _RefusingClient:
    """A Fabric client that fails the test if publication reaches it."""

    api_base_url = "https://api.invalid/v1"
    token = "token"
    timeout = 30

    def paged(self, path, *, key, not_found_empty=False):
        raise AssertionError(f"unexpected read of {path}")

    def get_json(self, path):
        raise AssertionError(f"unexpected read of {path}")

    def request(self, method, path, *, payload=None, expected=()):
        raise AssertionError(f"unexpected {method} {path}")


@weaver_test()
def test_a_rejected_candidate_never_starts_a_build(tmp_path, monkeypatch):
    unrelated = _project(tmp_path / "parcel", OTHER_PROJECT)
    resolve = env_mod.project_root
    started: list[object] = []
    monkeypatch.setattr(
        env_mod, "project_root", lambda: resolve(_installed(unrelated / ".venv"))
    )
    monkeypatch.setattr(env_mod.subprocess, "run", lambda *a, **k: started.append(a))

    with pytest.raises(CommandError, match=NO_CHECKOUT):
        env_mod.publish_environment(
            "Analytics", "Runtime", dev=True, client=_RefusingClient()
        )

    assert started == [], "a build backend ran against an unidentified project"
