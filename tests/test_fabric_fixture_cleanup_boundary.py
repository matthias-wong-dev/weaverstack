"""Module cleanup is registered before a Fabric fixture can mutate its targets."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

from support.weaver_test import weaver_test


@weaver_test()
def test_module_cleanup_retains_reverse_dependency_order(monkeypatch):
    import sys

    path = Path(__file__).parent / "fabric" / "conftest.py"
    spec = importlib.util.spec_from_file_location("cleanup_fixtures", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    finalizers = []
    cleaned = []
    request = SimpleNamespace(addfinalizer=finalizers.append)
    register = module.fabric_lakehouse_cleanup.__wrapped__(request, cleaned.append)
    register("producer", "consumer", "producer")
    assert cleaned == []
    assert len(finalizers) == 2
    for finalizer in reversed(finalizers):
        finalizer()
    assert cleaned == ["consumer", "producer"]
