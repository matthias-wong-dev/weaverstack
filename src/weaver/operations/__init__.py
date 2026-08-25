"""The four things a caller asks Weaver to do, one module each.

Deliberately re-exporting nothing. The public names and the module names are
the same words, so a package that exported ``build`` would shadow ``build.py``
and ``weaver.operations.build`` would mean the callable in one line and the
module in the next, which is the collision this package was made to remove.

So each name has exactly one meaning in each namespace:

.. code-block:: text

    weaver.build                     the callable a caller uses
    weaver.operations.build          the module it lives in
    weaver.operations.build.build    the same callable, named in full

Result and report types live beside the operation that produces them, and
:mod:`weaver` re-exports the ones a caller handles.
"""

from __future__ import annotations

__all__: list[str] = []
