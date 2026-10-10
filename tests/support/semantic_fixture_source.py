"""Retain a configured model's native SQL Import source in Fabric fixtures."""

import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass, field

import pytest

from weaver.fabric.client import FabricError
from weaver.semantic_models import TmdlDefinition
from weaver.semantic_models.definition import decode_model, decode_parts, encode_parts
from weaver.semantic_models.m_source import relation, sql_database
from weaver.semantic_models.wipe import reset_definition

# The fixed PBIP fixture has two source-free literal Import partitions.
_LOCAL_LITERAL_M = frozenset(
    {
        "let\n    Source = #table(type table [Id = Int64.Type, ProductId = Int64.Type, Amount = Currency.Type], {{1, 10, 12.5}, {2, 20, 7.5}})\nin\n    Source",
        'let\n    Source = #table(type table [ProductId = Int64.Type, ProductName = text], {{10, "Cake"}, {20, "Coffee"}})\nin\n    Source',
    }
)


def _hash(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, default=str).encode()
    ).hexdigest()


def stable_connection(value):
    return {
        key: value.get(key)
        for key in (
            "id",
            "connectivityType",
            "gatewayId",
            "connectionDetails",
            "displayName",
            "privacyLevel",
            "credentialDetails",
        )
    }


def _expression(value):
    if isinstance(value, list):
        value = "\n".join(value)
    assert isinstance(value, str) and value.strip(), "Configured source has no native M"
    return value.strip()


def _observed_sources(observed):
    model = observed["model"]
    assert not model.get("expressions") and not model.get("dataSources"), (
        "Configured fixture source requires literal SQL Import navigation"
    )
    values = []
    for table in model.get("tables", []):
        for partition in table.get("partitions", []):
            source = partition["source"]
            if source["type"] == "calculated":
                continue
            assert partition.get("mode") == "import" and source["type"] == "m", (
                "Configured fixture source mode changed"
            )
            expression = _expression(source["expression"])
            if expression not in _LOCAL_LITERAL_M:
                values.append(expression)
    assert values, "Configured fixture source was removed"
    assert len(set(values)) == 1, "Configured fixture source navigation changed"
    return values[0]


def _desired_sources(parts):
    model = TmdlDefinition(parts).model
    assert not model.expressions and not model.dataSources, (
        "Configured fixture source requires literal SQL Import navigation"
    )
    values = []
    for table in model.tables:
        for partition in table.partitions:
            if partition.sourceType == "calculated":
                continue
            assert partition.mode == "import" and partition.sourceType == "m", (
                "Configured fixture source mode changed; use a separately approved target"
            )
            expression = _expression(partition.source)
            if expression not in _LOCAL_LITERAL_M:
                values.append(expression)
    assert values, "Configured fixture source was removed"
    assert len(set(values)) == 1, "Configured fixture source navigation changed"
    return values[0]


@dataclass
class ConfiguredSemanticSource:
    model: object
    original: dict
    native_parts: dict
    expression: str
    binding: list
    shared: dict
    evidence: list = field(default_factory=list)
    touched: bool = False
    execute: object = None
    request: object = None

    @classmethod
    def capture(cls, model, *, name="The configured model"):
        original = model.get_definition()
        native = decode_parts({**model.get_definition(format="TMDL"), "format": "TMDL"})
        connections = model.get_connections()
        observed = decode_model(original)
        expression = _observed_sources(observed)
        assert len(connections) == 1 and connections[0].get("id"), (
            "Configured fixture source requires an explicit connection"
        )
        # The guarded wipe keeps the source, so the model must admit one.
        reset_definition(
            "Fixture",
            observed,
            parts=native,
            preserve_data_source=True,
            connections=connections,
        )
        assert _desired_sources(native) == expression, (
            "Native source differs from observed source"
        )
        assert connections[0]["connectivityType"] == "ShareableCloud", (
            "Configured fixture source requires ShareableCloud SQL Import"
        )
        try:
            shared = model.fabric.get_json(f"connections/{connections[0]['id']}")
        except FabricError as error:
            if error.status_code != 403:
                raise
            pytest.skip(
                f"{name}'s connection is not readable by this sign-in; "
                "sign in as the model's owner"
            )
        instance = cls(
            model,
            original,
            native,
            expression,
            [stable_connection(value) for value in connections],
            stable_connection(shared),
        )
        instance.record("admission", original, connections, shared)
        return instance

    def record(self, stage, definition, connections, shared):
        self.evidence.append(
            {
                "stage": stage,
                "definition": _hash(definition),
                "source": _hash(self.expression),
                "binding": _hash([stable_connection(value) for value in connections]),
                "connection_metadata": _hash(connections),
                "shared_metadata": _hash(shared),
                "shared_configuration": _hash(stable_connection(shared)),
            }
        )

    def verify(self, stage):
        definition = self.model.get_definition()
        connections = self.model.get_connections()
        shared = self.model.fabric.get_json(f"connections/{self.binding[0]['id']}")
        assert _observed_sources(decode_model(definition)) == self.expression, (
            "Configured fixture source changed"
        )
        assert [stable_connection(value) for value in connections] == self.binding, (
            "Configured fixture binding changed"
        )
        assert stable_connection(shared) == self.shared, (
            "Configured shared connection changed"
        )
        self.record(stage, definition, connections, shared)

    def guard_definition(self, definition):
        observed = definition.get("format") == "TMSL"
        expression = (
            _observed_sources(decode_model(definition))
            if observed
            else _desired_sources(decode_parts(definition))
        )
        assert expression == self.expression, (
            "Configured fixture source changed; use a separately approved target"
        )

    def require_metadata(self, metadata):
        found = relation(self.expression)
        server, database = sql_database(list(found.root_tokens))
        expected = {
            "server": server.value,
            "database": database.value,
            "schema": found.schema,
            "object": found.object,
        }
        assert all(
            metadata[key].casefold() == value.casefold()
            for key, value in expected.items()
        ), "Configured fixture source differs from the Session catalogue source"

    def require_preservation(self, preserve):
        assert preserve is True, (
            "Configured fixture wipes require preserve_data_source=True; use a separately approved target"
        )

    def run(self, stage, operation, *args, **kwargs):
        self.verify(f"before-{stage}")
        try:
            return operation(*args, **kwargs)
        finally:
            self.verify(f"after-{stage}")

    def wipe(self, *args, preserve_data_source=True, **kwargs):
        import weaver

        self.require_preservation(preserve_data_source)
        return self.run("wipe", weaver.wipe, *args, preserve_data_source=True, **kwargs)

    def attach(self, session):
        self.execute = session.execute_mutation
        self.request = self.model.fabric.request

        def request(method, path, *args, **kwargs):
            assert not (
                method.upper() != "GET" and path.rstrip("/").endswith("/bindConnection")
            ), "Configured fixture cannot rebind its source connection"
            if method.upper() == "POST" and path.rstrip("/").endswith(
                f"/{self.model.model_id}/updateDefinition"
            ):
                self.verify("before-definition-update")
                self.guard_definition(kwargs["payload"]["definition"])
                self.touched = True
            return self.request(method, path, *args, **kwargs)

        self.model.fabric.request = request

        def execute(plan, payloads=None, **options):
            self.verify("before-mutation")
            for _, _, action in plan.actions():
                if action.executor not in {"semantic_model", "semantic_wipe"}:
                    continue
                spec = json.loads(payloads[action.payload])
                assert not spec.get("bind_data_sources"), (
                    "Fixture cannot change source bindings"
                )
                if action.executor == "semantic_wipe":
                    self.require_preservation(spec["preserve_data_source"])
                self.guard_definition(spec["definition"])
            self.touched = True
            try:
                return self.execute(plan, payloads, **options)
            finally:
                self.verify("after-mutation")

        session.execute_mutation = execute

    def detach(self, session):
        session.execute_mutation = self.execute
        self.model.fabric.request = self.request

    def restore(self, settle):
        if not self.touched:
            self.verify("unmutated-cleanup")
            return
        self.verify("before-cleanup")
        definition = encode_parts(self.native_parts)
        self.guard_definition(definition)
        settle(self.model)
        self.model.update_definition(definition, allow_purge_data=True, timeout=300)
        self.verify("restored-definition")
        self.model.refresh(timeout=300)
        self.verify("cleanup")
        assert decode_model(self.model.get_definition()) == decode_model(self.original)


@contextmanager
def guarded_source(model, session, backup, settle, *, name):
    """Guard the configured model's source for the test, then restore it."""

    source = ConfiguredSemanticSource.capture(model, name=name)
    backup.write_text(json.dumps(source.original), encoding="utf-8")
    print(f"Original semantic definition: {backup}")
    source.attach(session)
    try:
        yield source
    finally:
        source.detach(session)
        try:
            source.restore(settle)
            if source.touched:
                print(f"Restored semantic model {model.workspace_id}/{model.model_id}")
        finally:
            print("SEMANTIC_SOURCE_EVIDENCE " + json.dumps(source.evidence))
