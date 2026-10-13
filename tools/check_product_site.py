#!/usr/bin/env python3
"""Check the static product routes, shared navigation and benchmark facts."""

from __future__ import annotations

import argparse
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit


@dataclass
class Element:
    tag: str
    attrs: dict[str, str]
    children: list[Element] = field(default_factory=list)
    text: list[str] = field(default_factory=list)

    def descendants(self, tag=None):
        for child in self.children:
            if tag is None or child.tag == tag:
                yield child
            yield from child.descendants(tag)

    def content(self):
        return " ".join(" ".join(self.text).split())


class Document(HTMLParser):
    def __init__(self, text):
        super().__init__(convert_charrefs=True)
        self.root = Element("document", {})
        self.stack = [self.root]
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        node = Element(tag, {name: value or "" for name, value in attrs})
        self.stack[-1].children.append(node)
        if tag not in {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                break

    def handle_data(self, data):
        for node in self.stack:
            node.text.append(data)


def one(nodes, label):
    assert len(nodes) == 1, f"{label}: expected one, got {len(nodes)}"
    return nodes[0]


def with_class(root, name):
    return [n for n in root.descendants() if name in n.attrs.get("class", "").split()]


def route_file(root, path):
    candidate = root / unquote(path).lstrip("/")
    return candidate / "index.html" if path.endswith("/") else candidate


def run(root: Path, developer_site: Path | None):
    expected_features = [
        ("Lakehouse", "/lakehouse/"),
        ("Warehouse", "/warehouse/"),
        ("Semantic Models", "/semantic-models/"),
        ("Reports", "/reports/"),
    ]
    routes, titles, descriptions = set(), set(), set()
    documents = {}
    footer_links = None
    docs_links = set()
    for file in sorted(root.rglob("index.html")):
        route = "/" + str(file.relative_to(root)).removesuffix("index.html")
        doc = Document(file.read_text()).root
        documents[route] = doc
        routes.add("https://weaverstack.dev" + route)
        title = one(list(doc.descendants("title")), f"{route} title").content()
        assert title and title not in titles, f"{route}: absent/duplicate title"
        titles.add(title)
        description = one(
            [
                n
                for n in doc.descendants("meta")
                if n.attrs.get("name") == "description"
            ],
            f"{route} description",
        ).attrs["content"]
        assert description and description not in descriptions, (
            f"{route}: absent/duplicate description"
        )
        descriptions.add(description)
        assert one(list(doc.descendants("h1")), f"{route} H1").content()
        canonical = one(
            [n for n in doc.descendants("link") if n.attrs.get("rel") == "canonical"],
            f"{route} canonical",
        )
        assert canonical.attrs["href"] == "https://weaverstack.dev" + route
        main = one(list(doc.descendants("main")), f"{route} main")
        assert len(main.content()) > 200, f"{route}: insufficient body"
        nav = one(with_class(doc, "nav-links"), f"{route} navigation")
        features = one(with_class(nav, "features-nav"), f"{route} Features")
        assert (
            one([n for n in features.children if n.tag == "a"], "Features label").attrs[
                "href"
            ]
            == "/features/"
        )
        disclosure = one(list(features.descendants("details")), "native disclosure")
        summary = one(list(disclosure.descendants("summary")), "disclosure summary")
        assert summary.attrs.get("aria-label")
        children = [(n.content(), n.attrs["href"]) for n in disclosure.descendants("a")]
        assert children == expected_features, (
            f"{route}: wrong feature children {children}"
        )
        primary = [n.attrs["href"] for n in nav.children if n.tag == "a"]
        assert primary.count("/scale/") == 1, f"{route}: scale not independent"
        assert not ({x[1] for x in expected_features} & set(primary)), (
            f"{route}: item as top-level tab"
        )
        footer = one(list(doc.descendants("footer")), f"{route} footer")
        current = [(n.content(), n.attrs["href"]) for n in footer.descendants("a")]
        if footer_links is None:
            footer_links = current
        assert current == footer_links, f"{route}: inconsistent footer"
        ids = [n.attrs["id"] for n in doc.descendants() if "id" in n.attrs]
        assert len(ids) == len(set(ids)), f"{route}: duplicate IDs"
        for node in doc.descendants():
            href = node.attrs.get("href") or node.attrs.get("src")
            if not href:
                continue
            url = urlsplit(urljoin("https://weaverstack.dev" + route, href))
            if url.netloc == "docs.weaverstack.dev":
                docs_links.add(url.geturl())
                if developer_site:
                    target = route_file(developer_site, url.path)
                    assert target.is_file(), (
                        f"{route}: missing developer destination {href}"
                    )
                continue
            if url.netloc != "weaverstack.dev":
                continue
            target = route_file(root, url.path)
            assert target.is_file(), f"{route}: missing local destination {href}"
            if url.fragment and target.suffix == ".html":
                dest = Document(target.read_text()).root
                assert url.fragment in {
                    n.attrs.get("id") for n in dest.descendants()
                }, f"{route}: missing fragment {href}"
    sitemap = ET.parse(root / "sitemap.xml")
    listed = {
        n.text
        for n in sitemap.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
    }
    assert listed == routes, f"Sitemap mismatch: {listed ^ routes}"
    assert "Disallow: /" not in (root / "robots.txt").read_text()
    for name in ("CNAME", ".nojekyll", "favicon.png", "og.png", "site.webmanifest"):
        assert (root / name).is_file(), f"Missing {name}"
    assert (root / "CNAME").read_text().strip() == "weaverstack.dev"
    hub = documents["/features/"]
    assert {
        n.attrs.get("href")
        for n in one(list(hub.descendants("main")), "hub main").descendants("a")
    } >= {x[1] for x in expected_features}
    data = json.loads((root / "assets/data/scale-benchmark.json").read_text())
    assert (
        data["counts"]["tables"] + data["counts"]["views"] + data["counts"]["folders"]
        == data["counts"]["declared_objects"]
        == 3000
    )
    rows = {
        n.attrs["data-benchmark"]: n
        for n in documents["/scale/"].descendants("tr")
        if "data-benchmark" in n.attrs
    }
    assert set(rows) == {x["id"] for x in data["operations"]}
    for fact in data["operations"]:
        row = rows[fact["id"]]
        assert row.attrs["data-seconds"] == str(fact["seconds"])
        assert (
            fact["display"] == f"{fact['seconds'] // 60}m {fact['seconds'] % 60:02d}s"
        )
        cells = [n.content() for n in row.children if n.tag in {"td", "th"}]
        assert cells == [fact["label"], fact["display"], fact["context"]], (fact, cells)
    print(
        json.dumps(
            {
                "product_routes_checked": len(documents),
                "shared_navigation_and_footer": "pass",
                "local_links_fragments_and_metadata": "pass",
                "benchmark_operations_checked": len(rows),
                "developer_destinations": sorted(docs_links),
                "developer_link_state": "staged files checked"
                if developer_site
                else "not checked: provide --developer-site after the docs build",
                "browser_accessibility": "not established by HTML checks",
            },
            indent=2,
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--site", type=Path, default=Path(__file__).resolve().parents[1] / "docs"
    )
    parser.add_argument("--developer-site", type=Path)
    args = parser.parse_args()
    run(
        args.site.resolve(),
        args.developer_site.resolve() if args.developer_site else None,
    )


if __name__ == "__main__":
    main()
