"""Local Power BI source-project context."""

import json
import posixpath
from dataclasses import dataclass
from typing import Mapping

from .declaration.model import REPORT, SEMANTIC_MODEL, WeaverItemId
from .errors import ConfigError, DiscoveryError


@dataclass(frozen=True)
class SemanticDefinition:
    model: WeaverItemId
    model_path: str | None
    model_tmdl: str | None


@dataclass(frozen=True)
class PowerBIProject:
    path: str
    definitions: Mapping[WeaverItemId, SemanticDefinition]
    items: tuple[WeaverItemId, ...]
    report_paths: tuple[str, ...] = ()

    def model_at(self, path):
        """The logical model whose native directory is ``path``.

        A model declared only by ``<Name>.tmdl`` owns ``<Name>.SemanticModel``
        beside that declaration.
        """
        for item, definition in self.definitions.items():
            if path == (
                definition.model_path or f"{self.path}/{item.item_name}.SemanticModel"
            ):
                return item
        return None


@dataclass(frozen=True)
class ReportContribution:
    path: str
    model: WeaverItemId | None
    parts: Mapping[str, bytes]
    binding: Mapping[str, str] | None = None

    @property
    def source_signature(self):
        import hashlib

        from .semantic_models.compiler import content_signature

        return content_signature(
            {
                "model": str(self.model) if self.model else None,
                "parts": {
                    p: hashlib.sha256(b).hexdigest() for p, b in self.parts.items()
                },
            }
        )

    def effective_signature(self, *, binding=None):
        from .semantic_models.compiler import content_signature

        return content_signature(
            {
                "compiler": 2 if binding is not None else 1,
                "source": self.source_signature,
                "binding": binding,
            }
        )

    @property
    def signature(self):
        return self.effective_signature(binding=self.binding)


def artifact_paths(paths, suffix):
    return sorted(
        {
            "/".join(p.split("/")[: i + 1])
            for p in paths
            for i, component in enumerate(p.split("/"))
            if component.endswith(suffix)
        }
    )


def discover_projects(paths, directories=()):
    projects = {}
    entries = set(paths) | set(directories)
    for name in sorted(
        {
            p.split("/")[1]
            for p in entries
            if p.startswith("PowerBI/") and len(p.split("/")) > 2
        }
    ):
        prefix = f"PowerBI/{name}/"
        local = {p for p in paths if p.startswith(prefix)}
        declarations = sorted(
            p for p in local if p.endswith(".tmdl") and "/" not in p[len(prefix) :]
        )
        native = {p for p in entries if p.startswith(prefix)}
        models = artifact_paths(native, ".SemanticModel")
        reports = artifact_paths(native, ".Report")
        names = {}
        for path in models + declarations:
            model_name = path.rsplit("/", 1)[1][
                : -14 if path.endswith(".SemanticModel") else -5
            ]
            prior = names.get(model_name.casefold())
            if prior and (
                prior[0] != model_name
                or any(
                    p.endswith(".SemanticModel") == path.endswith(".SemanticModel")
                    for p in prior[1]
                )
            ):
                raise DiscoveryError(
                    f"SemanticModel/{model_name}: duplicate logical item at {prior[1][0]} and {path}"
                )
            if prior:
                prior[1].append(path)
            else:
                names[model_name.casefold()] = (model_name, [path])
        definitions = {}
        for model_name, _ in sorted(names.values()):
            item = WeaverItemId(SEMANTIC_MODEL, model_name)
            definitions[item] = SemanticDefinition(
                item,
                next(
                    (p for p in models if p.rsplit("/", 1)[1][:-14] == model_name), None
                ),
                next(
                    (p for p in declarations if p.rsplit("/", 1)[1][:-5] == model_name),
                    None,
                ),
            )
        items = list(definitions) + [
            WeaverItemId(REPORT, p.rsplit("/", 1)[1][:-7]) for p in reports
        ]
        projects[name] = PowerBIProject(
            prefix.rstrip("/"), definitions, tuple(sorted(items)), tuple(reports)
        )
    return projects


def read_reports(projects, *, paths, root, store):
    reports = {}
    for project in projects.values():
        local = {p for p in paths if p.startswith(project.path + "/")}
        report_paths = project.report_paths
        for pbip in sorted(p for p in local if p.endswith(".pbip")):
            try:
                declaration = json.loads(
                    store.read(root.join(*pbip.split("/"))).decode("utf-8-sig")
                )
                for artifact in declaration["artifacts"]:
                    relative = artifact["report"]["path"]
                    if (
                        not isinstance(relative, str)
                        or not relative
                        or "\\" in relative
                        or relative.startswith("/")
                    ):
                        raise ValueError("expected a relative Report path")
                    target = posixpath.normpath(
                        posixpath.join(posixpath.dirname(pbip), relative)
                    )
                    if target not in report_paths:
                        raise ValueError(
                            f"{relative}: expected a Report inside {project.path}"
                        )
            except (KeyError, TypeError, ValueError) as exc:
                raise ConfigError(f"{pbip}: invalid PBIP reference: {exc}") from exc
        for path in report_paths:
            item = WeaverItemId(REPORT, path.rsplit("/", 1)[1][:-7])
            parts = {
                p[len(path) + 1 :]: store.read(root.join(*p.split("/")))
                for p in sorted(paths)
                if p.startswith(path + "/") and "/.pbi/" not in p
            }
            definition_path = path + "/definition.pbir"
            try:
                definition = json.loads(parts["definition.pbir"].decode("utf-8-sig"))
                ref = definition["datasetReference"]
                if not isinstance(ref, dict) or set(ref) not in (
                    {"byPath"},
                    {"byConnection"},
                ):
                    raise ValueError(
                        "expected a native byPath or byConnection reference"
                    )
                kind = next(iter(ref))
                field = "path" if kind == "byPath" else "connectionString"
                if (
                    not isinstance(ref[kind], dict)
                    or not isinstance(ref[kind].get(field), str)
                    or not ref[kind][field]
                ):
                    raise ValueError(f"expected a native {field}")
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                raise ConfigError(f"{definition_path}: {exc}") from exc
            model = WeaverItemId(SEMANTIC_MODEL, item.item_name)
            if model not in project.definitions:
                model = None
                if kind == "byPath":
                    relative = ref[kind][field]
                    model = project.model_at(
                        posixpath.normpath(posixpath.join(path, relative))
                    )
                    if model is None:
                        raise ConfigError(
                            f"{definition_path}: byPath {relative!r} names no "
                            f"semantic model in {project.path}. Point it at a model "
                            "in this project, name the Report after a model, or use "
                            "a byConnection reference"
                        )
            reports[item] = ReportContribution(path, model, parts)
    return reports
