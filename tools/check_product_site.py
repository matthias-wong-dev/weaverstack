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
    forwarding = {}
    primary_links = None
    contract = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "acceptance/features-unification/feature-contract.json"
        ).read_text()
    )
    documents = {}
    footer_links = None
    docs_links = set()
    for file in sorted(root.rglob("index.html")):
        route = "/" + str(file.relative_to(root)).removesuffix("index.html")
        doc = Document(file.read_text()).root
        if route in {x[1] for x in expected_features}:
            destination = "https://docs.weaverstack.dev" + route
            canonical = one(
                [
                    n
                    for n in doc.descendants("link")
                    if n.attrs.get("rel") == "canonical"
                ],
                f"{route} shim canonical",
            )
            assert canonical.attrs.get("href") == destination
            robots = one(
                [n for n in doc.descendants("meta") if n.attrs.get("name") == "robots"],
                f"{route} shim robots",
            )
            assert "noindex" in robots.attrs.get("content", "").split(",")
            pointers = [
                n for n in doc.descendants("a") if "data-forward-destination" in n.attrs
            ]
            assert (
                one(pointers, f"{route} visible pointer").attrs.get("href")
                == destination
            )
            assert not list(doc.descendants("header")), (
                f"{route}: full product page retained"
            )
            assert len(doc.content()) < 1000, f"{route}: forwarding body is not minimal"
            assert not list(doc.descendants("script")), (
                f"{route}: pointer depends on JavaScript"
            )
            assert not any(
                n.attrs.get("http-equiv", "").lower() == "refresh"
                for n in doc.descendants("meta")
            ), f"{route}: fragment-losing refresh"
            if developer_site:
                assert route_file(developer_site, route).is_file(), (
                    f"{route}: pointer destination missing"
                )
            forwarding[route] = destination
            continue
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
        assert not with_class(nav, "features-nav"), (
            f"{route}: retired feature submenu must be replaced by one ordinary Features link"
        )
        feature_links = [
            n
            for n in nav.children
            if n.tag == "a" and n.attrs.get("href") == "/features/"
        ]
        one(feature_links, f"{route} primary Features link")
        assert not list(nav.descendants("details")), f"{route}: product submenu remains"
        primary = [n.attrs["href"] for n in nav.children if n.tag == "a"]
        labelled = [
            (n.content(), n.attrs["href"]) for n in nav.children if n.tag == "a"
        ]
        if primary_links is None:
            primary_links = labelled
        assert labelled == primary_links, f"{route}: inconsistent primary navigation"
        assert primary.count("/story/") == 1, f"{route}: missing Story link"
        if route == "/features/":
            assert feature_links[0].attrs.get("aria-current") == "page"
        scripts = [
            n
            for n in doc.descendants("script")
            if n.attrs.get("src") and n.attrs["src"].endswith("assets/site.js")
        ]
        one(scripts, f"{route} shared analytics/navigation asset")
        social = {
            n.attrs.get("property") or n.attrs.get("name"): n.attrs.get("content")
            for n in doc.descendants("meta")
        }
        assert social.get("og:url") == "https://weaverstack.dev" + route
        assert social.get("og:title") and social.get("og:description")
        image = urlsplit(social.get("og:image", ""))
        assert (
            image.netloc == "weaverstack.dev" and route_file(root, image.path).is_file()
        ), f"{route}: missing social image"
        assert social.get("twitter:card") == "summary_large_image"
        assert social.get("twitter:title") and social.get("twitter:description")
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
                    if url.fragment:
                        dest = Document(target.read_text()).root
                        assert unquote(url.fragment) in {
                            n.attrs.get("id") for n in dest.descendants()
                        }, f"{route}: missing developer fragment {href}"
                continue
            if url.netloc != "weaverstack.dev":
                continue
            assert url.path not in {x[1] for x in expected_features}, (
                f"{route}: stale retired product link {href}"
            )
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
    showcase = one(list(hub.descendants("main")), "Features main")
    assert "features-showcase" in showcase.attrs.get("class", "").split()
    groups = {
        n.attrs["id"]: n
        for n in showcase.descendants("section")
        if "data-feature-group" in n.attrs
    }
    assert set(groups) == set(contract["groups"]), (
        "Feature section IDs missing or duplicated"
    )
    jumps = one(with_class(showcase, "feature-jumps"), "feature section links")
    assert {n.attrs.get("href") for n in jumps.descendants("a")} == {
        "#" + key for key in groups
    }
    rows = [n for n in showcase.descendants() if "data-feature" in n.attrs]
    identities = [n.attrs["data-feature"] for n in rows]
    assert len(identities) == len(set(identities)) == len(contract["features"]), (
        "Feature inventory missing or duplicated"
    )
    assert set(identities) == {x["id"] for x in contract["features"]}, (
        "Feature inventory differs from approved scope"
    )
    for group in groups.values():
        assert one(list(group.descendants("h2")), "feature group heading").content()
        assert not list(group.descendants("details")), (
            "Feature depth hidden in a disclosure"
        )
        group_rows = [n for n in group.descendants() if "data-feature" in n.attrs]
        expected = {
            x["id"]
            for x in contract["features"]
            if str(x["group"]) == group.attrs["data-feature-group"]
        }
        assert {n.attrs["data-feature"] for n in group_rows} == expected, (
            "Capability placed in wrong stage"
        )
        for row in group_rows:
            heading = one(list(row.descendants("h3")), "visible feature label")
            assert len(heading.content()) >= 8, "Feature label is not meaningful"
            paragraph = one(list(row.descendants("p")), "feature explanation")
            assert 35 <= len(paragraph.content()) <= 260, (
                "Feature explanation absent or too long"
            )
            assert not any(
                "hidden" in n.attrs
                or "display:none" in n.attrs.get("style", "").replace(" ", "")
                for n in (row, heading, paragraph)
            ), "Feature text hidden"
            feature = next(
                x for x in contract["features"] if x["id"] == row.attrs["data-feature"]
            )
            assert (
                one(list(heading.descendants("a")), "feature owner link").attrs.get(
                    "href"
                )
                in feature["urls"]
            ), "Wrong technical owner"
        assert with_class(group, "group-docs") or with_class(group, "spotlight-docs"), (
            "Feature group has no task/reference access"
        )
    spotlights = [
        n for n in showcase.descendants("figure") if "data-spotlight" in n.attrs
    ]
    assert [n.attrs["data-spotlight"] for n in spotlights] == contract["spotlights"], (
        "Four spotlights missing or out of progressive order"
    )
    for node in spotlights:
        assert one(list(node.descendants("h3")), "spotlight heading").content()
        assert list(node.descendants("figcaption")), (
            "Spotlight lacks an accessible explanation"
        )
    diagram = one(with_class(spotlights[0], "fabric-dag"), "single Fabric graph")
    nodes = [
        n.attrs["data-dag-node"]
        for n in diagram.descendants()
        if "data-dag-node" in n.attrs
    ]
    assert nodes == [
        "folder",
        "python-table",
        "spark-table",
        "warehouse-table",
        "model-refresh",
    ], "Graph topology missing or reordered"
    edges = {
        n.attrs["data-dag-edge"]
        for n in diagram.descendants()
        if "data-dag-edge" in n.attrs
    }
    assert edges == {f"{a}:{b}" for a, b in zip(nodes, nodes[1:])}, (
        "Disconnected managed graph"
    )
    boundaries = [n for n in diagram.descendants() if "data-boundary" in n.attrs]
    assert (
        len(boundaries) == 1
        and boundaries[0].attrs["data-boundary"] == "Lakehouse:Warehouse"
    ), "Graph must cross one explicit Lakehouse/Warehouse boundary"
    assert all(
        term in diagram.content()
        for term in (
            "Python",
            "Spark SQL",
            "T-SQL",
            "Refresh",
            "Logical Shortcut",
            "SQL endpoint readiness",
        )
    )
    assert not with_class(diagram, "pipeline"), "Invented pipeline glue in graph"
    js = (root / "assets/site.js").read_text()
    assert js.count("https://plausible.io/js/pa-bPlUSh6aQE0FF8b9FruQj.js") == 1, (
        "Analytics loader drift"
    )
    assert js.count("window.plausible.init();") == 1, "Analytics initialisation drift"
    assert "features-nav" not in js, "Dead submenu JavaScript remains"
    data = json.loads((root / "assets/data/scale-benchmark.json").read_text())
    assert (
        data["counts"]["tables"] + data["counts"]["views"] + data["counts"]["folders"]
        == data["counts"]["declared_objects"]
        == 3000
    )
    for fact in data["operations"]:
        assert (
            fact["display"] == f"{fact['seconds'] // 60}m {fact['seconds'] % 60:02d}s"
        )
    scale = one(list(documents["/scale/"].descendants("main")), "scale main")
    results = {
        n.attrs["data-scale-result"]: n.content()
        for n in scale.descendants()
        if "data-scale-result" in n.attrs
    }
    assert set(results) == {"first-load", "incremental"}
    operations = {entry["id"]: entry for entry in data["operations"]}
    measured = [n for n in scale.descendants() if "data-benchmark-operation" in n.attrs]
    covered = set()
    for node in measured:
        ids = node.attrs["data-benchmark-operation"].split()
        seconds = [int(value) for value in node.attrs["data-benchmark-seconds"].split()]
        assert seconds == [operations[key]["seconds"] for key in ids]
        covered.update(ids)
    assert {"first-load", "day-1", "day-2", "day-3"} <= covered
    assert "F64" in scale.content() and "synthetic" in scale.content().lower()
    assert not any("/pull/" in n.attrs.get("href", "") for n in scale.descendants("a"))
    print(
        json.dumps(
            {
                "product_routes_checked": len(documents),
                "noindex_forwarding_routes_checked": len(forwarding),
                "visible_features_checked": len(rows),
                "feature_spotlights_checked": len(spotlights),
                "shared_navigation_and_footer": "pass",
                "local_links_fragments_and_metadata": "pass",
                "benchmark_operations_checked": len(data["operations"]),
                "rounded_public_scale_results": "pass",
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
