"""Semantic source contributions read through the repository's Store."""

from __future__ import annotations

import json
import posixpath
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import yaml

from ..declaration.metadata import _UniqueKeyLoader
from ..errors import ConfigError, MetadataError
from .compiler import compile_model, content_signature, leaf_properties
from .tmdl import _parse_file


@dataclass(frozen=True)
class SemanticContribution:
    model: dict
    sources: Mapping[str, bytes]
    provenance: Mapping[str, dict]
    properties: dict

    @property
    def signature(self):
        return content_signature(
            {"compiler": 1, "definition": self.model, "properties": self.properties}
        )


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
    base = None
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
        base = {"model": {}}
        references = []
        for path in definition_paths:
            before = leaf_properties(base)
            _parse_file(Path(path), base, references, text=read(path))
            for key, value in leaf_properties(base).items():
                if key not in before or before[key] != value:
                    provenance[key] = {"source": path, "reason": "PBIP"}
        names = {t["name"] for t in base["model"].get("tables", [])}
        if len(set(references)) != len(references) or any(
            n not in names for n in references
        ):
            raise ConfigError(f"{pbip}: duplicate or unresolved TMDL table reference")
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
    if base is None and organisation is None and local is None:
        raise ConfigError(f"{item}: provide a PBIP or addon.yml")
    try:
        model = compile_model(
            item.item_name,
            base=base,
            organisation=organisation,
            item=local,
            provenance=provenance,
            organisation_source=org_path,
            item_source=item_path,
        )
    except ConfigError as exc:
        raise ConfigError(f"{item} ({org_path}, {item_path}): {exc}") from exc
    return SemanticContribution(model, sources, provenance, properties)
