"""Exercise the final semantic compiler with explicit offline metadata."""

from weaver.semantic_models.binding import bind_semantic_sources
from weaver.semantic_models.fragments import source_table


def compile_repository(repository, sources=None):
    observed = dict(sources or {})
    for contribution in repository.semantic_models.values():
        for table, reference in contribution.source_references.items():
            if source_table(contribution.parts, table).get("partitions"):
                observed.setdefault(reference, {"reference": reference})
    return bind_semantic_sources(repository, observed, repository.semantic_models)
