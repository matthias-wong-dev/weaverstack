"""Package-owned resources Weaver ships and installs like ordinary content.

``catalogue`` holds the built-in SES repository declaring Weaver's own control
tables. It is deliberately *not* a Python package: it is a repository, read
through the ordinary SES reader, and an ``__init__.py`` sitting in it would
travel into the Weaver Lakehouse as a support file and into that repository's
signature. Resources are reached through :mod:`importlib.resources` from here, so
the same path works in a source tree and in an installed wheel.
"""
