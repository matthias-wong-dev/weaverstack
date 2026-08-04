"""A developer running a deployed load primitive in Fabric, by hand.

This is a *primitive* test, and its claim is developer-facing: someone in a
Fabric session can import a deployed object and run its load, with no planner,
no catalogue orchestration and no estate-level entry point in the way.

That is also why it is ``hosted``. The subject is the API the *wheel
installed in the session* offers — that `.load()` exists there and behaves —
rather than the load semantics underneath it, which are proved locally in
``tests/spark`` for a fraction of the cost.

These are the claims local Spark cannot make, and the branch learned that the
expensive way. In the emulator ``Files`` is an ordinary directory, so a folder
load against ``tmp_path`` exercises the same code with none of the risk; and
deployed modules sit flat in a temp directory, so an import that could never
work from a real runtime tree succeeds. Fabric differs in exactly the two ways
that broke:

.. code-block:: text

    Files is object storage      -> a Folder needs a mount, not a URL
    the tree is a deployed package -> imports resolve as Files.* and lib.*

So this file asserts only what needs OneLake to be true. Everything about *load
semantics* — rejection, thresholds, incremental policy — is proved locally in
``tests/spark``, and repeating it here would buy nothing for several minutes.

One submission, one evidence payload, per the suite's rule: every question about
the installed estate goes in one body and the assertions run locally against
what it brings back.

It therefore carries ``fabric`` and ``hosted``: the first says where the
resources are, the second says where Weaver runs. The platform question
underneath it — what a mount is — is a ``fabric and remote`` test of its own and
runs without a publish.
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.fabric, pytest.mark.hosted]


#: Everything asked of the installed estate, in one round trip. It imports from
#: the deployed tree the way the deployed tree is meant to be imported, loads the
#: folder through the resolved OneLake path, and runs the generated Spark SQL
#: program from the file the installer wrote.
BODY = r'''
import os, sys

from weaver import lakehouse_for

# The body's `target` is an ItemRef — a name, not a destination. Resolving it is
# the orchestrator's move, and the one an object never makes for itself.
destination = lakehouse_for(resolver, target)
results = {}

# The deployed runtime tree, reached as Python must reach it: through the mount
# Weaver makes of the root it resolved, not through /lakehouse/default, which
# names only whatever a notebook happened to attach.
root = destination.files_root() + "/_/Load"
sys.path.insert(0, root)
results["deployed"] = sorted(os.listdir(root))

# The import a deployed tree is laid out for: `Files.*`, because the authored
# path is reproduced verbatim beneath the root — `Files/Raw__CustomerCsv.py`
# stays where it was written, so the module name says the same thing it did.
from Files.Raw__CustomerCsv import Raw__CustomerCsv

results["imported"] = Raw__CustomerCsv.__name__

# What the folder reads is deployed, not placed here: `lib/` travels whole, so
# a helper module's data file arrives beside the module that reads it.
results["lib"] = sorted(os.listdir(os.path.join(root, "lib", "data")))

# The folder load, writing ordinary files to OneLake through the mount.
export = Raw__CustomerCsv(spark, lakehouse=destination)
results["folder"] = export.load().as_row()
results["folder_path_is_local"] = not export.local_path().startswith("abfss://")
results["folder_path_is_spark"] = export.path().startswith("abfss://")
results["published"] = sorted(os.listdir(export.local_path()))

emit(results)
'''


def test_a_developer_can_run_a_deployed_folder_load_primitive(fabric_lakehouse_estate):
    """Import the object the installer deployed, call ``.load()``, and be done.

    The folder is the subject because it is the primitive that most needs a real
    Lakehouse: its authored code writes with ``open()``, which cannot address an
    ``abfss://`` URL at all. Before the files root existed this reported success
    and wrote into a local directory called ``abfss:/…``.
    """

    env = fabric_lakehouse_estate.env

    seen = env.run_python(BODY)

    # The tree the installer wrote, laid out as authored: the item's own modules
    # at the root, and the `Files/` segment preserved rather than flattened.
    assert "Files" in seen["deployed"]
    assert "DWG__Customer.py" in seen["deployed"]
    # `lib/` travels whole. This fixture's holds only a CSV, so a `.py` filter
    # left it out entirely and the folder's read() found nothing to copy.
    assert "lib" in seen["deployed"]
    assert seen["lib"] == ["customers.csv"]
    # The authored path is reproduced verbatim, so the import reads the same.
    assert seen["imported"] == "Raw__CustomerCsv"
    # Two spellings of one location, because two things read them.
    assert seen["folder_path_is_local"] is True
    assert seen["folder_path_is_spark"] is True
    # And the files reached OneLake rather than a directory named after a URL.
    assert seen["folder"]["succeeded"] is True
    assert seen["published"], "the folder load published nothing"
