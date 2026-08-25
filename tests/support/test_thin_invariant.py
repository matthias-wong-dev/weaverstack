"""The thin primitives meet the contract a run calls them through.

They stand where a deployed module stands, and they are only ever dispatched
in Fabric, so nothing in the pure suite exercises the method a run invokes on
them. That gap cost a Fabric round trip once: the run's lower interface was
renamed and these were left implementing the upper one, and the first thing to
notice was a real workspace twenty minutes later.

So the contract is read from the production dispatcher rather than restated here,
and each stand-in is held to it.
"""

from __future__ import annotations

import ast
import inspect

from support.thin import ARTEFACTS, VALIDATIONS
from support.weaver_test import weaver_test


def _called_on_the_primitive() -> set[str]:
    """The methods ``python_primitive`` invokes on the object it constructed.

    Read from the source, because the point is to notice a rename. Asking the
    class would agree with any name it currently has.
    """

    from weaver.run import dispatch

    tree = ast.parse(inspect.getsource(dispatch.python_primitive))
    return {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "primitive"
    }


@weaver_test()
def test_a_run_calls_the_lower_interface_and_only_that():
    """``load()`` records, so a run calling it would be a second writer."""

    assert _called_on_the_primitive() == {"_load"}


@weaver_test()
def test_every_thin_load_implements_what_the_run_calls():
    (wanted,) = _called_on_the_primitive()

    missing = [
        name for name, source in ARTEFACTS.items() if f"def {wanted}(" not in source
    ]

    assert not missing, f"these stand-ins do not implement {wanted}(): {missing}"


@weaver_test()
def test_no_thin_load_implements_the_recording_wrapper():
    """A stand-in with a ``load()`` would pass a run that called the wrong one."""

    offering = [name for name, source in ARTEFACTS.items() if "def load(" in source]

    assert not offering


@weaver_test()
def test_every_thin_validation_is_read_rather_than_loaded():
    """A validation's lower interface is ``read()``, and that has not moved."""

    missing = [
        name for name, source in VALIDATIONS.items() if "def read(" not in source
    ]

    assert not missing


@weaver_test()
def test_every_stand_in_meets_the_constructor_contract():
    """``cls(spark, lakehouse=...)`` is the whole contract a deployed class has."""

    for name, source in {**ARTEFACTS, **VALIDATIONS}.items():
        assert "def __init__(self, spark, lakehouse=None):" in source, name


__all__: tuple = ()
