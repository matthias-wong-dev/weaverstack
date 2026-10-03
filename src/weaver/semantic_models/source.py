"""Semantic source contributions read through the repository's Store."""

from __future__ import annotations

import json
import posixpath
from dataclasses import dataclass, field
from typing import Mapping

import yaml

from ..declaration.metadata import _UniqueKeyLoader
from ..errors import ConfigError, MetadataError
from .compiler import content_signature


@dataclass(frozen=True)
class SemanticContribution:
    parts: Mapping[str, bytes]
    sources: Mapping[str, bytes]
    provenance: Mapping[str, dict]
    requested: dict = field(default_factory=dict)
    owned: tuple[str, ...] = ()
    source_references: Mapping[str, str] = field(default_factory=dict)
    source_bindings: Mapping[str, dict] = field(default_factory=dict)
    expression_sources: Mapping[str, dict] = field(default_factory=dict)

    @property
    def dependencies(self):
        from .lineage import dependency_references

        return dependency_references(self.source_references, self.source_bindings)

    @property
    def properties(self):
        return json.loads(self.parts["definition.pbism"].decode("utf-8-sig"))

    @property
    def signature(self):
        import hashlib

        value = {
            "compiler": 2,
            "parts": {p: hashlib.sha256(b).hexdigest() for p, b in self.parts.items()},
        }
        if self.source_references or self.source_bindings:
            value["source_references"] = dict(self.source_references)
            value["source_bindings"] = dict(self.source_bindings)
        return content_signature(value)


def read_semantic_contribution(item, *, root, store, paths):
    prefix = str(item) + "/"
    available = {p for p in paths if p.startswith(prefix)}
    sources = {}

    def read(path):
        if path not in available and path != "SemanticModel/addon.yml":
            raise ConfigError(f"{path}: referenced semantic source is missing")
        sources[path] = store.read(root.join(*path.split("/")))
        return sources[path].decode("utf-8-sig")

    def referenced(parent, relative):
        if (
            not isinstance(relative, str)
            or "\\" in relative
            or relative.startswith("/")
        ):
            raise ConfigError(f"{parent}: expected a relative PBIP path")
        path = posixpath.normpath(posixpath.join(parent, relative))
        if not path.startswith(prefix):
            raise ConfigError(f"{path}: PBIP reference must stay inside {item}")
        return path

    def addon(path):
        if path not in paths:
            return None
        try:
            value = yaml.load(read(path), Loader=_UniqueKeyLoader)
        except (yaml.YAMLError, MetadataError) as exc:
            raise ConfigError(f"{path}: invalid addon YAML: {exc}") from exc
        if not isinstance(value, dict):
            raise ConfigError(f"{path}: addon must be a mapping")
        return value

    unsupported = sorted(
        p
        for p in available
        if p[len(prefix) :].split("/")[0]
        in {
            "tests",
            "assumptions",
            "schemas",
            "Tables",
            "Files",
            "programmables",
            "lib",
        }
        or p.endswith((".dax", ".sql", ".py"))
    )
    if unsupported:
        raise ConfigError(
            f"{unsupported[0]}: this semantic authored form is not supported yet"
        )
    pbips = sorted(
        p for p in available if p.endswith(".pbip") and "/" not in p[len(prefix) :]
    )
    if len(pbips) > 1:
        raise ConfigError(f"{item}: expected exactly one PBIP base")
    parts = {}
    provenance = {}
    properties = {"version": "4.2", "settings": {}}
    if pbips:
        pbip = pbips[0]
        try:
            project = json.loads(read(pbip))
            models = set()
            for artifact in project["artifacts"]:
                report = referenced(str(item), artifact["report"]["path"])
                definition = json.loads(read(report + "/definition.pbir"))
                ref = definition["datasetReference"]
                if set(ref) != {"byPath"}:
                    raise ConfigError(
                        f"{pbip}: PBIP must reference a local semantic model"
                    )
                models.add(referenced(report, ref["byPath"]["path"]))
            if len(models) != 1:
                raise ConfigError(
                    f"{pbip}: PBIP must reference exactly one semantic model"
                )
            model_path = models.pop()
            properties = json.loads(read(model_path + "/definition.pbism"))
        except (KeyError, TypeError, ValueError) as exc:
            raise ConfigError(
                f"{pbip}: invalid PBIP reference or definition: {exc}"
            ) from exc
        if not isinstance(properties, dict) or not isinstance(
            properties.get("version"), str
        ):
            raise ConfigError(
                f"{model_path}/definition.pbism: expected versioned properties"
            )
        definition_paths = sorted(
            p
            for p in available
            if p.startswith(model_path + "/definition/") and "/.pbi/" not in p
        )
        if not definition_paths or any(
            not p.endswith(".tmdl") for p in definition_paths
        ):
            raise ConfigError(f"{model_path}: expected a supported TMDL definition")
        parts["definition.pbism"] = sources[model_path + "/definition.pbism"]
        for path in definition_paths:
            read(path)
            parts[path[len(model_path) + 1 :]] = sources[path]
        other_models = [
            p
            for p in available
            if p.endswith("/definition.pbism") and p != model_path + "/definition.pbism"
        ]
        if other_models:
            raise ConfigError(f"{other_models[0]}: unreferenced semantic model")
    elif any(p.endswith((".tmdl", ".bim", ".pbism")) for p in available):
        raise ConfigError(f"{item}: semantic base files require a PBIP reference")
    org_path = "SemanticModel/addon.yml"
    item_path = prefix + "addon.yml"
    organisation = addon(org_path)
    local = addon(item_path)
    if not parts and organisation is None and local is None:
        raise ConfigError(f"{item}: provide a PBIP or addon.yml")
    contribution = SemanticContribution(parts, sources, provenance)
    if organisation is not None or local is not None:
        from .patching import apply_addons

        try:
            contribution = apply_addons(
                contribution,
                item.item_name,
                ((organisation, org_path), (local, item_path)),
            )
        except ConfigError as exc:
            raise ConfigError(f"{item} ({org_path}, {item_path}): {exc}") from exc
    return contribution
