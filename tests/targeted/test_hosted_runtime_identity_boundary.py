"""Hosted Build admits matching published protocol, version and capabilities."""

import pytest
from support.weaver_test import weaver_test
from test_mutation_archive_install import build_plan_fixture

from weaver.errors import BuildError


@weaver_test()
def test_runtime_identity_uses_the_existing_development_publication_fingerprint():
    from weaver.fabric.environment import local_weaver
    from weaver.sessions.hosted_runtime import identity, require_compatible

    observed = identity()
    assert observed["version"] == local_weaver().version
    assert observed["protocol"] == 1 and observed["format_version"] == 5
    assert observed["deltalake"] == "1.6.6"
    assert "materialise_mirror" in observed["executors"]
    plan, _ = build_plan_fixture()
    assert require_compatible(observed, expected=observed, plan=plan) == observed


@weaver_test()
@pytest.mark.parametrize(
    "defect", ["missing", "version", "protocol", "format", "writer", "capability"]
)
def test_incompatible_hosted_runtime_is_refused_with_publication_guidance(defect):
    from weaver.sessions.hosted_runtime import identity, require_compatible

    expected = identity()
    observed = dict(expected)
    if defect == "missing":
        observed = None
    elif defect == "version":
        observed["version"] += "1"
    elif defect == "protocol":
        observed["protocol"] += 1
    elif defect == "format":
        observed["format_version"] -= 1
    elif defect == "writer":
        observed["deltalake"] = "0.0.0"
    else:
        observed["executors"] = []
    plan, _ = build_plan_fixture()
    with pytest.raises(BuildError, match="[Pp]ublish"):
        require_compatible(observed, expected=expected, plan=plan)
