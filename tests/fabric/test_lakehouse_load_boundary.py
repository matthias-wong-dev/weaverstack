"""The Lakehouse load primitives, against real OneLake.

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

``published_weaver`` alone, never also ``fabric``. The markers are opted into by
name and neither implies the other, so carrying both would let ``-m fabric``
select this — and ``-m fabric`` promises a workspace and *nothing published*.
What needs the wheel is what this is about: the installed package acquiring its
own capabilities. The platform question underneath it — what a mount is — is a
``fabric`` test of its own, and runs in a minute without a publish.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.published_weaver


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

# The data file this fixture's folder reads. Placed by the test rather than by
# the installer: the deployed tree currently carries only `.py` support files,
# so a module that reads a data file beside it finds nothing. That gap is real
# and reported separately — it is not what this test is about, and standing on
# it would stop the folder claim being proved at all.
data = os.path.join(root, "lib", "data")
os.makedirs(data, exist_ok=True)
with open(os.path.join(data, "customers.csv"), "w") as handle:
    handle.write("CustomerId,CustomerName\n1,Ada\n2,Grace\n")

# The folder load, writing ordinary files to OneLake through the mount.
export = Raw__CustomerCsv(spark, lakehouse=destination)
results["folder"] = export.load().as_row()
results["folder_path_is_local"] = not export.local_path().startswith("abfss://")
results["folder_path_is_spark"] = export.path().startswith("abfss://")
results["published"] = sorted(os.listdir(export.local_path()))

emit(results)
'''


def test_the_lakehouse_load_primitives_reach_onelake(fabric_lakehouse_estate):
    """The deployed tree imports, and a Folder writes files that actually land.

    The folder is the subject: its authored code writes with ``open()``, which
    cannot address an ``abfss://`` URL at all. Before the files root existed
    this reported success and wrote into a local directory called ``abfss:/…``.
    """

    env = fabric_lakehouse_estate.env

    seen = env.run_python(BODY)

    # The tree the installer wrote, laid out as authored: the item's own modules
    # at the root, and the `Files/` segment preserved rather than flattened.
    assert "Files" in seen["deployed"]
    assert "DWG__Customer.py" in seen["deployed"]
    # `lib/` is absent, and that is a finding rather than an expectation: this
    # fixture's lib/ holds only a CSV, and the deployed tree carries `.py`
    # support files alone. A module that reads a data file beside it therefore
    # finds nothing — see the note in BODY, which supplies the file itself.
    assert "lib" not in seen["deployed"]
    # The authored path is reproduced verbatim, so the import reads the same.
    assert seen["imported"] == "Raw__CustomerCsv"
    # Two spellings of one location, because two things read them.
    assert seen["folder_path_is_local"] is True
    assert seen["folder_path_is_spark"] is True
    # And the files reached OneLake rather than a directory named after a URL.
    assert seen["folder"]["succeeded"] is True
    assert seen["published"], "the folder load published nothing"
