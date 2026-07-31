"""Build-lifecycle assertions that only read an installed estate.

Split out of ``test_build_bundle`` because of what they cost rather than what
they say. Both simply look at an estate that has been built — where its objects
landed, and what the install left behind — and neither changes anything. Sharing
one module-scoped install between them is two fewer Lakehouse installs per run,
and a Fabric install is 75–123s.

They cannot share a module with the tests that still take a fresh target:
``fabric_build_env`` empties the target Lakehouse on the way in, and both it and
``lakehouse_estate`` are handed the same one, so a function-scoped environment
running between two module-scoped assertions would empty the estate underneath
them. Separate modules keep the two lifecycles apart.
"""

from __future__ import annotations


def test_nothing_is_built_in_the_weaver_lakehouse(lakehouse_estate):
    """The control plane is not the destination, and a build must not treat it as one.

    This is the assertion the old two-part names made impossible. The session is
    attached to the Weaver Lakehouse; an unqualified ``CREATE TABLE DWG.Customer``
    lands there; and the old test then read ``DWG.Customer`` back through the same
    session and found it. Asking the *Weaver* Lakehouse directly is what closes it.
    """

    env = lakehouse_estate.env
    weaver = env.weaver_destination

    assert not env.schema_exists("DWG", destination=weaver)
    assert not env.schema_exists("Raw", destination=weaver)
    # And it did land in the destination, so the absence above is not vacuous.
    assert env.schema_exists("DWG")


def test_install_report_is_written_into_the_bundle(lakehouse_estate):
    """A bundle carries the record of its own installation."""

    env = lakehouse_estate.env

    assert env.store.exists(lakehouse_estate.bundle.location.join("install-report.yml"))
