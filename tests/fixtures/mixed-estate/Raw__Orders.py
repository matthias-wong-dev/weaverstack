"""
Folder ID: Raw.Orders

Description: Raw order files as delivered.

Lineage: A nightly drop.

File key: "*.csv"
"""

from weaver import Folder


class Raw__Orders(Folder):
    def read(self):
        raise RuntimeError("read() must not run during build")
