"""The minimal public surface at checkpoint 0."""

from __future__ import annotations

import weaver
from weaver.errors import CommandError, WeaverError


def test_version_is_exposed():
    assert weaver.__version__


def test_error_hierarchy_has_one_root():
    assert issubclass(CommandError, WeaverError)
    assert issubclass(WeaverError, Exception)
