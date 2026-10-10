"""The Catalogue Dashboard renderer's pure logic, under Node's test runner."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest
from support.weaver_test import weaver_test

# Named files: Node on the Windows runner reads a directory argument as a module.
TESTS = sorted((Path(__file__).parent / "js").glob("*.test.js"))


def _node() -> str | None:
    return os.environ.get("WEAVER_NODE") or shutil.which("node")


@weaver_test()
def test_the_renderer_logic_passes_its_node_tests():
    node = _node()
    if node is None:
        pytest.skip("Node.js is not on PATH; set WEAVER_NODE to run the renderer tests")
    result = subprocess.run(
        [node, "--test", *map(str, TESTS)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
