"""Read the bounded native fragments required by explicit source ownership."""

from .tmdl import Document, PackageEditor


def expression_text(document, node):
    value = node.value
    if value is None:
        value = node.text.partition("=")[2].strip()
    if value and value != "```":
        return value
    lines = document.lines[node.header + 1 : node.expression_end]
    if value == "```" and lines and lines[-1].strip() == "```":
        closing = lines.pop()
        prefix = closing[: len(closing) - len(closing.lstrip(" \t"))]
        return "\n".join(
            line[len(prefix) :].rstrip("\r\n")
            if line.startswith(prefix)
            else line.rstrip("\r\n")
            for line in lines
        )
    while lines and not lines[-1].strip():
        lines.pop()
    prefixes = [
        line[: len(line) - len(line.lstrip(" \t"))] for line in lines if line.strip()
    ]
    prefix = prefixes[0] if prefixes else ""
    for candidate in prefixes[1:]:
        while prefix and not candidate.startswith(prefix):
            prefix = prefix[:-1]
    return "\n".join(
        line[len(prefix) :].rstrip("\r\n")
        if line.startswith(prefix)
        else line.rstrip("\r\n")
        for line in lines
    )


def scalar(text):
    value = text.strip()
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1].replace('""', '"')
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1].replace("''", "'")
    return value


def properties(document, node, names):
    result = {}
    if "description" in names and node.description_start < node.header:
        result["description"] = "\n".join(
            line.strip()[3:].lstrip(" ")
            for line in document.lines[node.description_start : node.header]
        )
    for child in node.children:
        if not child.kind and child.name in names:
            result[child.name] = scalar(child.text.partition(":")[2])
    return result


def source_table(parts, name):
    result = {"name": name}
    for document, table in PackageEditor(parts).locations((("table", name),)):
        result["name"] = table.name
        result.update(properties(document, table, {"description"}))
        for node in table.children:
            if node.kind == "column":
                result.setdefault("columns", []).append(
                    {
                        "name": node.name,
                        **properties(
                            document, node, {"description", "dataType", "sourceColumn"}
                        ),
                    }
                )
            elif node.kind == "partition":
                source = {"type": node.value}
                member = {
                    "name": node.name,
                    **properties(document, node, {"mode"}),
                    "source": source,
                }
                for child in node.children:
                    if child.name == "source" and not child.kind:
                        if node.value == "entity":
                            source.update(
                                properties(
                                    document,
                                    child,
                                    {"schemaName", "entityName", "expressionSource"},
                                )
                            )
                        else:
                            source["expression"] = expression_text(document, child)
                result.setdefault("partitions", []).append(member)
    return result


def source_context(parts, table):
    context = {}
    for document, model in PackageEditor(parts).locations(()):
        context.update(properties(document, model, {"defaultMode"}))
    referenced = {
        p.get("source", {}).get("expressionSource") for p in table.get("partitions", [])
    }
    for filename, content in parts.items():
        if not filename.startswith("definition/") or not filename.endswith(".tmdl"):
            continue
        document = Document(filename, content)
        for node in document.spans:
            if node.kind == "expression":
                value = {"name": node.name}
                if node.name in referenced:
                    value.update(kind="m", expression=expression_text(document, node))
                    value.update(properties(document, node, {"kind"}))
                context.setdefault("expressions", []).append(value)
    return context
