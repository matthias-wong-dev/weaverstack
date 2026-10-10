"""Semantic source contributions read through the repository's Store.

Raw project reads retain native parts and same-name overlay bytes without
policy or annotation execution. Composition owns layer ordering; final binding
owns annotation execution, source metadata and generation.
"""

from __future__ import annotations

import json
import posixpath
from dataclasses import dataclass, field
from typing import Mapping

from ..errors import ConfigError
from .compiler import content_signature


@dataclass(frozen=True)
class SemanticContribution:
    parts: Mapping[str, bytes]
    sources: Mapping[str, bytes]
    provenance: Mapping[str, dict]
    requested: dict = field(default_factory=dict)
    owned: tuple[str, ...] = ()
    absent: tuple[tuple[tuple[str, str], ...], ...] = ()
    source_references: Mapping[str, str] = field(default_factory=dict)
    source_bindings: Mapping[str, dict] = field(default_factory=dict)
    expression_sources: Mapping[str, dict] = field(default_factory=dict)
    table_order: tuple[str, ...] | None = None

    #: Bind data sources to their connections once the definition is deployed.
    bind_data_sources: bool = False
    #: The annotation classes this contribution compiles with; not desired state.
    annotations: object = field(default=None, compare=False, repr=False)
    compilation: object = field(default=None, compare=False, repr=False)

    @property
    def table_names(self):
        from .tmdl import Document

        if self.table_order is not None:
            return self.table_order
        names = {}
        paths = list(self.parts)
        if "definition/model.tmdl" in paths:
            paths.remove("definition/model.tmdl")
            paths.insert(0, "definition/model.tmdl")
        for path in paths:
            if path.startswith("definition/") and path.endswith(".tmdl"):
                for span in Document(path, self.parts[path]).spans:
                    if span.kind == "table" and len(span.path) == 1:
                        names.setdefault(span.name.casefold(), span.name)
        return tuple(names.values())

    @property
    def artifact_signatures(self):
        import hashlib

        groups = {}
        natives = tuple(
            p.rsplit("/", 1)[0] for p in self.sources if p.endswith("/definition.pbism")
        )
        for path, content in self.sources.items():
            native = next((p for p in natives if path.startswith(p + "/")), None)
            if native:
                key = ("Definition", native.rsplit("/", 1)[-1])
                relative = path[len(native) + 1 :]
            else:
                key = (
                    "Policy" if path == "PowerBI/policy.tmdl" else "Definition",
                    path.rsplit("/", 1)[-1],
                )
                relative = key[1]
            groups.setdefault(key, {})[relative] = hashlib.sha256(content).hexdigest()
        for path, content in getattr(self.annotations, "sources", {}).items():
            name = path.rsplit("/", 1)[-1]
            groups[("Annotations", name)] = {name: hashlib.sha256(content).hexdigest()}
        return {key: content_signature(parts) for key, parts in sorted(groups.items())}

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
            "compiler": 3,
            "parts": {p: hashlib.sha256(b).hexdigest() for p, b in self.parts.items()},
            "table_order": self.table_names,
        }
        if self.absent:
            value["absent"] = self.absent
        if self.source_references or self.source_bindings:
            value["source_references"] = dict(self.source_references)
            value["source_bindings"] = dict(self.source_bindings)
        return content_signature(value)


def read_semantic_contribution(
    item, *, root, store, paths, annotations=None, project=None, raw=False
):
    prefix = (project.path if project else str(item)) + "/"
    definition = project.definitions[item] if project else None
    available = {p for p in paths if p.startswith(prefix)}
    if project:
        available = {
            p
            for p in available
            if not any(p.startswith(report + "/") for report in project.report_paths)
        }
    sources = {}

    def read(path):
        if path not in available and path not in {"PowerBI/policy.tmdl"}:
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

    for legacy in ("SemanticModel/addon.yml", prefix + "addon.yml"):
        if legacy in paths:
            raise ConfigError(
                f"{legacy}: addon.yml is no longer supported; use PowerBI/policy.tmdl or <model-name>.tmdl"
            )

    # Validations are ordinary documents, read by the repository and kept out
    # of the model's definition.
    available = [
        p
        for p in available
        if p[len(prefix) :].split("/")[0] not in {"tests", "assumptions"}
    ]
    unsupported = sorted(
        p
        for p in available
        if p[len(prefix) :].split("/")[0]
        in {
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
    if len(pbips) > 1 and not project:
        raise ConfigError(f"{item}: expected exactly one PBIP base")
    parts = {}
    provenance = {}
    properties = {"version": "4.2", "settings": {}}
    if definition and definition.model_path:
        model_path = definition.model_path
        property_path = model_path + "/definition.pbism"
        try:
            properties = json.loads(read(property_path))
        except ValueError as exc:
            raise ConfigError(f"{property_path}: invalid properties: {exc}") from exc
        if not isinstance(properties, dict) or not isinstance(
            properties.get("version"), str
        ):
            raise ConfigError(f"{property_path}: expected versioned properties")
        parts["definition.pbism"] = sources[property_path]
        definition_paths = sorted(
            p
            for p in available
            if p.startswith(model_path + "/definition/") and "/.pbi/" not in p
        )
        if not definition_paths or any(
            not p.endswith(".tmdl") for p in definition_paths
        ):
            raise ConfigError(f"{model_path}: expected a supported TMDL definition")
        for path in definition_paths:
            read(path)
            parts[path[len(model_path) + 1 :]] = sources[path]
    elif pbips and not project:
        pbip = pbips[0]
        try:
            pbip_project = json.loads(read(pbip))
            models = set()
            for artifact in pbip_project["artifacts"]:
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
    elif not project and any(
        p.endswith((".tmdl", ".bim", ".pbism"))
        and p != prefix + item.item_name + ".tmdl"
        for p in available
    ):
        raise ConfigError(f"{item}: semantic base files require a PBIP reference")
    extensions = []
    for path in (
        ("PowerBI/policy.tmdl", definition.model_tmdl)
        if project
        else ("PowerBI/policy.tmdl", prefix + item.item_name + ".tmdl")
    ):
        if raw and path == "PowerBI/policy.tmdl":
            continue
        if path in paths:
            read(path)
            extensions.append((sources[path], path))
    if not parts and not extensions:
        raise ConfigError(f"{item}: provide a PBIP or <model-name>.tmdl")
    contribution = SemanticContribution(parts, sources, provenance)
    if raw:
        return contribution
    if extensions:
        from .extensions import apply_extensions

        contribution = apply_extensions(contribution, item.item_name, extensions)
    from .annotation import prepare_annotations

    return prepare_annotations(contribution, annotations)
