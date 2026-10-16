"""Notebook workflows parse the same requests without the optional CLI."""

from __future__ import annotations

import importlib.util

from support.weaver_test import weaver_test


@weaver_test()
def test_shared_request_parser_is_available_to_native_notebook_workflows():
    assert importlib.util.find_spec("weaver.command_parser") is not None
    from weaver.command_parser import build_parser

    args = build_parser().parse_args(
        [
            "load",
            "Warehouse/Operations",
            "--name",
            "Parcel[.]StatusEvent",
            "--ancestors",
            "--workspace",
            "Parcel Development",
            "--workspace-config",
            "workspace-development.yml",
            "--concurrency",
            "2",
        ]
    )
    assert args.command == "load"
    assert args.items == ["Warehouse/Operations"]
    assert args.names == ["Parcel[.]StatusEvent"]
    assert args.ancestors is True
    assert args.workspace == "Parcel Development"
    assert args.workspace_config == "workspace-development.yml"
    assert args.concurrency == 2


@weaver_test()
def test_shared_parser_refuses_invalid_concurrency_with_an_argument_error():
    import pytest

    from weaver.command_parser import build_parser

    with pytest.raises(SystemExit) as error:
        build_parser().parse_args(["build", "--concurrency", "0"])
    assert error.value.code == 2
