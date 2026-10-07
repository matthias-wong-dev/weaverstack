"""Compose local raw definitions before policy and annotation execution."""

from dataclasses import replace

from ..declaration.model import SEMANTIC_MODEL, WeaverItemId
from ..errors import ConfigError
from .annotation import prepare_annotations
from .extensions import apply_extensions
from .fragments import expression_text, scalar
from .source import SemanticContribution, read_semantic_contribution
from .tmdl import Document


def base_names(fragments):
    names = ()
    for path, content in fragments:
        if path.endswith(".tmdl"):
            document = Document(path, content)
            for node in document.spans:
                if (
                    node.kind == "annotation"
                    and node.name == "Weaver.BaseSemanticModels"
                ):
                    names = tuple(
                        scalar(line)
                        for line in expression_text(document, node).splitlines()
                        if line.strip()
                    )
    return names


def read_project_models(project, *, root, store, paths, annotations):
    raw = {
        item: read_semantic_contribution(
            item, root=root, store=store, paths=paths, project=project, raw=True
        )
        for item in project.definitions
    }
    local = {}
    for item, contribution in raw.items():
        definition = project.definitions[item]
        fragments = list(contribution.parts.items())
        if definition.model_tmdl:
            fragments.append(
                (definition.model_tmdl, contribution.sources[definition.model_tmdl])
            )
        local[item] = base_names(fragments)

    def ordered(item):
        result = []
        seen = set()

        def visit(current, chain):
            if current in chain:
                cycle = chain[chain.index(current) :] + (current,)
                raise ConfigError(
                    f"{project.path}: composition cycle: {' -> '.join(str(i) for i in cycle)}"
                )
            if current in seen:
                raise ConfigError(
                    f"{project.path}: repeated base {current.item_name!r} in {item}"
                )
            seen.add(current)
            for name in local[current]:
                base = WeaverItemId(SEMANTIC_MODEL, name)
                if base not in local:
                    raise ConfigError(
                        f"{current} references missing base {name!r} in {project.path}; bases must be local to this project"
                    )
                if base == current:
                    raise ConfigError(f"{project.path}: self-reference in {current}")
                visit(base, chain + (current,))
            result.append(current)

        visit(item, ())
        return result

    policy = "PowerBI/policy.tmdl"
    policy_content = (
        store.read(root.join(*policy.split("/"))) if policy in paths else None
    )
    models = {}
    for item in raw:
        contribution = SemanticContribution({}, {}, {})
        order = ordered(item)
        for current in order:
            source = raw[current]
            if source.parts:
                if not contribution.parts:
                    contribution = replace(contribution, parts=source.parts)
                else:
                    positions = {
                        name.casefold(): i for i, name in enumerate(source.table_names)
                    }

                    def position(pair):
                        path, content = pair
                        tables = [
                            n.name
                            for n in Document(path, content).spans
                            if n.kind == "table"
                            and len(n.path) == 1
                            and not n.reference
                        ]
                        return (
                            1 if path == "definition/model.tmdl" else 0,
                            min((positions[n.casefold()] for n in tables), default=-1),
                        )

                    fragments = sorted(
                        (
                            (p, b)
                            for p, b in source.parts.items()
                            if p.startswith("definition/")
                            and p.endswith(".tmdl")
                            and p != "definition/database.tmdl"
                        ),
                        key=position,
                    )
                    contribution = apply_extensions(
                        contribution,
                        item.item_name,
                        [
                            (
                                b,
                                next(
                                    (s for s in source.sources if s.endswith("/" + p)),
                                    p,
                                ),
                            )
                            for p, b in fragments
                        ],
                    )
                    metadata = {
                        p: b
                        for p, b in source.parts.items()
                        if p in {"definition.pbism", "definition/database.tmdl"}
                    }
                    contribution = replace(
                        contribution, parts={**contribution.parts, **metadata}
                    )
            contribution = replace(
                contribution, sources={**contribution.sources, **source.sources}
            )
            if current == item and policy_content is not None:
                contribution = replace(
                    contribution,
                    sources={**contribution.sources, policy: policy_content},
                )
                contribution = apply_extensions(
                    contribution, item.item_name, [(policy_content, policy)]
                )
            target = project.definitions[current].model_tmdl
            if target:
                contribution = apply_extensions(
                    contribution, item.item_name, [(source.sources[target], target)]
                )
        models[item] = prepare_annotations(contribution, annotations)
    return models
