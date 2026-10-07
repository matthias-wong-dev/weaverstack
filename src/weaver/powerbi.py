"""Local Power BI source-project context."""

import json
import posixpath
from dataclasses import dataclass
from typing import Mapping

from .declaration.model import REPORT, SEMANTIC_MODEL, WeaverItemId
from .errors import ConfigError


@dataclass(frozen=True)
class PowerBIProject:
    path: str
    model: WeaverItemId | None
    model_path: str | None
    model_tmdl: str | None
    items: tuple[WeaverItemId, ...]
    report_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReportContribution:
    path: str
    model: WeaverItemId
    parts: Mapping[str, bytes]
    binding: Mapping[str, str] | None = None

    @property
    def source_signature(self):
        import hashlib

        from .semantic_models.compiler import content_signature

        return content_signature(
            {
                "model": str(self.model),
                "parts": {
                    p: hashlib.sha256(b).hexdigest() for p, b in self.parts.items()
                },
            }
        )

    def effective_signature(self, *, binding=None):
        from .semantic_models.compiler import content_signature

        return content_signature(
            {
                "compiler": 1,
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
        names = {p.rsplit("/", 1)[1][:-14] for p in models} | {
            p.rsplit("/", 1)[1][:-5] for p in declarations
        }
        if len(models) > 1 or len(names) > 1:
            raise ConfigError(
                f"{prefix.rstrip('/')}: expected at most one local semantic model; found {', '.join(models + declarations)}"
            )
        model_path = models[0] if models else None
        model_tmdl = declarations[0] if declarations else None
        model_name = (
            model_path.rsplit("/", 1)[1][:-14]
            if model_path
            else model_tmdl.rsplit("/", 1)[1][:-5]
            if model_tmdl
            else None
        )
        model = WeaverItemId(SEMANTIC_MODEL, model_name) if model_name else None
        if reports and model is None:
            raise ConfigError(
                f"{prefix.rstrip('/')}: report-only/thin projects are not supported; declare one local semantic model"
            )
        items = ([model] if model else []) + [
            WeaverItemId(REPORT, p.rsplit("/", 1)[1][:-7]) for p in reports
        ]
        projects[name] = PowerBIProject(
            prefix.rstrip("/"),
            model,
            model_path,
            model_tmdl,
            tuple(sorted(items)),
            tuple(reports),
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
                if not isinstance(ref, dict) or set(ref) != {"byPath"}:
                    raise ValueError(
                        "thin/byConnection Reports are not supported; use a local byPath model"
                    )
                relative = ref["byPath"]["path"]
                if (
                    not isinstance(relative, str)
                    or not relative
                    or "\\" in relative
                    or relative.startswith("/")
                ):
                    raise ValueError("expected a relative local model path")
                expected = (
                    project.model_path
                    or f"{project.path}/{project.model.item_name}.SemanticModel"
                )
                if posixpath.normpath(posixpath.join(path, relative)) != expected:
                    raise ValueError(f"byPath must name the local model {expected}")
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                raise ConfigError(f"{definition_path}: {exc}") from exc
            reports[item] = ReportContribution(path, project.model, parts)
    return reports
