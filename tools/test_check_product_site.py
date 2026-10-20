"""Regressions for the intentional unified product-site invariants."""

import contextlib
import io
import shutil
import tempfile
import unittest
from pathlib import Path

from check_product_site import run, stamp_assets

ROOT = Path(__file__).resolve().parents[1] / "docs"


class ProductMigration(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="product-site-gates-")
        self.addCleanup(self.temporary.cleanup)
        self.site = Path(self.temporary.name) / "docs"
        shutil.copytree(ROOT, self.site)

    def check(self):
        with contextlib.redirect_stdout(io.StringIO()):
            run(self.site, None)

    def mutate(self, route, old, new):
        path = self.site / route / "index.html"
        text = path.read_text()
        self.assertIn(old, text)
        path.write_text(text.replace(old, new, 1))

    def test_accepts_canonical_pages_and_noindex_pointers(self):
        self.check()

    def test_rejects_a_stylesheet_link_not_versioned_to_its_content(self):
        css = self.site / "assets" / "site.css"
        css.write_text(css.read_text() + "\n.changed { color: red; }\n")
        with self.assertRaisesRegex(AssertionError, "not versioned"):
            self.check()
        stamp_assets(self.site)
        self.check()

    def test_rejects_a_link_to_the_retired_scale_page(self):
        self.mutate(
            "features",
            "<h2>Built for scale</h2>",
            '<h2>Built for scale</h2><a href="/scale/">old</a>',
        )
        with self.assertRaisesRegex(AssertionError, "stale retired product link"):
            self.check()

    def test_rejects_a_scale_pointer_that_is_a_full_page(self):
        path = self.site / "scale" / "index.html"
        path.write_text(path.read_text().replace("<main", "<header></header><main", 1))
        with self.assertRaisesRegex(AssertionError, "full product page retained"):
            self.check()

    def test_rejects_the_retired_feature_dropdown(self):
        self.mutate(
            "features",
            '<a href="/features/" aria-current="page">Features</a>',
            '<div class="features-nav"><a href="/features/">Features</a><details><summary>Items</summary></details></div>',
        )
        with self.assertRaisesRegex(AssertionError, "retired feature submenu"):
            self.check()

    def test_rejects_missing_capability(self):
        self.mutate("features", 'data-feature="agents"', 'data-former-feature="agents"')
        with self.assertRaisesRegex(AssertionError, "Feature inventory missing"):
            self.check()

    def test_rejects_duplicate_capability(self):
        self.mutate("features", 'data-feature="agents"', 'data-feature="tsql"')
        with self.assertRaisesRegex(AssertionError, "Feature inventory missing"):
            self.check()

    def test_rejects_disconnected_cross_engine_graph(self):
        self.mutate(
            "features",
            'data-dag-edge="spark-table:warehouse-table"',
            'data-former-edge="spark-table:warehouse-table"',
        )
        with self.assertRaisesRegex(AssertionError, "Disconnected managed graph"):
            self.check()

    def test_rejects_a_stale_product_link(self):
        self.mutate(
            "", 'href="https://docs.weaverstack.dev/lakehouse/"', 'href="/lakehouse/"'
        )
        with self.assertRaisesRegex(AssertionError, "stale retired product link"):
            self.check()

    def test_rejects_indexable_forwarding_page(self):
        self.mutate("lakehouse", 'content="noindex,follow"', 'content="index,follow"')
        with self.assertRaises(AssertionError):
            self.check()

    def test_accepts_editorial_changes_without_exact_sentence_lock(self):
        self.mutate(
            "features",
            "Develop and operate Weaver projects on Windows, macOS or Linux, or work entirely inside Fabric Notebooks.",
            "Work on Windows, macOS or Linux, or entirely inside Fabric Notebooks.",
        )
        self.check()


if __name__ == "__main__":
    unittest.main()
