"""Runtime 2.0 protocol and typed readback on both Session creation routes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest
from factories import bound_target
from support.weaver_test import weaver_test

from weaver.build_bundle import execute_install_action
from weaver.build_bundle.executors.base import InstallationContext, ResolvedTarget
from weaver.build_bundle.installer import Installer
from weaver.build_bundle.models import InstallAction
from weaver.declaration.metadata import AUDIT_LIVE_DELETE_DATETIME
from weaver.declaration.model import LAKEHOUSE
from weaver.declaration.source import read_source_document
from weaver.fabric import FabricResolver, OneLakeDfsClient
from weaver.sessions.delta_profile import (
    compile_delta_profile_v1,
    verify_delta_profile_v1,
)
from weaver.targets import ItemRef

SCHEMA = "WeaverProtocolQualification"
CASES = (
    (
        "scalar",
        "STRING",
        "CAST('hello' AS STRING)",
        "CAST(Value AS STRING)",
        "hello",
        False,
    ),
    (
        "timestamp_ntz",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_NTZ '2026-01-01 00:00:00'",
        "CAST(Value AS STRING)",
        "2026-01-01 00:00:00",
        False,
    ),
    (
        "variant",
        "VARIANT",
        "parse_json('{\"k\":1}')",
        "variant_get(Value, '$.k', 'BIGINT')",
        1,
        False,
    ),
    (
        "identity_ntz",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_NTZ '2026-01-01 00:00:00'",
        "CAST(Value AS STRING)",
        "2026-01-01 00:00:00",
        True,
    ),
    (
        "explicit_scalar",
        "STRING",
        "CAST('hello' AS STRING)",
        "CAST(Value AS STRING)",
        "hello",
        False,
    ),
)


def _source(name, dtype, expression, identity, explicit):
    policy = (
        "Delta minReaderVersion: 2\nDelta minWriterVersion: 5\n" if explicit else ""
    )
    identity_header = "Identity: Id\n" if identity else ""
    return read_source_document(
        f"{SCHEMA}.{name}.sql",
        (
            f"/*\nTable ID: {SCHEMA}.{name}\nDescription: Runtime protocol qualification\n"
            "Lineage: Inline scalar\nDependencies: []\n"
            f"{identity_header}{policy}Schema: {{Value: {dtype}}}\n*/\n"
            f"SELECT {expression} AS Value;\n"
        ).encode(),
        LAKEHOUSE,
    )


@pytest.fixture(scope="module")
def protocol_estate(
    request,
    fabric_workspace,
    fabric_client,
    fabric_target_lakehouse,
    weaver_session,
    livy_session,
):
    resolver = FabricResolver(fabric_workspace, client=fabric_client)
    item = ItemRef(fabric_target_lakehouse.name)
    destination = resolver.spark_destination(item)
    schema = destination.qualified_schema(SCHEMA)
    request.addfinalizer(
        lambda: weaver_session.execute_spark_sql(
            f"DROP SCHEMA IF EXISTS {schema} CASCADE", workspace=fabric_workspace
        )
    )
    weaver_session.execute_spark_sql_batch(
        [f"DROP SCHEMA IF EXISTS {schema} CASCADE", f"CREATE SCHEMA {schema}"],
        workspace=fabric_workspace,
    )
    installer = Installer(weaver_session).bind(fabric_workspace)
    direct_creator = installer.direct_delta_table_creator()
    allocations = {}

    def create_direct(qualified, columns, **options):
        allocation = direct_creator(qualified, columns, **options)
        allocations[qualified] = (
            compile_delta_profile_v1(columns, **options),
            allocation,
        )
        return allocation

    target = ResolvedTarget(
        bound=bound_target(id="protocol-target", item_id=fabric_target_lakehouse.name),
        lakehouse=item,
        location=resolver.lakehouse_spark_location(item),
        destination=destination,
    )
    context = InstallationContext(
        create_delta_table=installer.delta_table_creator(),
        create_direct_delta_table=create_direct,
        spark_sql=installer.spark_sql(),
        spark_sql_batch=installer.spark_sql_batch(),
        resolver=resolver,
        store=OneLakeDfsClient(),
        target=target,
        targets={target.bound.id: target},
    )
    requests = []
    results = {}
    for route in ("direct", "spark"):
        current = (
            context
            if route == "direct"
            else replace(context, create_direct_delta_table=None)
        )
        for case, dtype, expression, read, expected, identity in CASES:
            name = f"{route}_{case}"
            source = _source(
                name, dtype, expression, identity, case == "explicit_scalar"
            )
            payload = source.create_ddl(destination=destination).content.encode()
            audit = json.loads(payload)["audit_columns"]
            assert len(audit) == 3
            insert_columns = ", ".join(
                f"`{column}`" for column in ["Value", *(entry[0] for entry in audit)]
            )
            insert_values = ", ".join(
                (
                    expression,
                    "current_timestamp()",
                    "current_timestamp()",
                    f"TIMESTAMP '{AUDIT_LIVE_DELETE_DATETIME}'",
                )
            )
            action = InstallAction(
                id=name,
                kind="build_table",
                executor="spark_table",
                resource_node_id=None,
                payload=f"payload/{name}.json",
                payload_sha256=hashlib.sha256(payload).hexdigest(),
            )
            result = execute_install_action(action, payload, context=current)
            assert result.status == "succeeded", json.dumps(result.to_mapping())
            assert (destination.qualify(SCHEMA, name) in allocations) == (
                route == "direct"
            )
            results[name] = result
            requests.append(
                {
                    "name": name,
                    "qualified": destination.qualify(SCHEMA, name),
                    "case": case,
                    "dtype": dtype.lower(),
                    "expression": expression,
                    "insert_columns": insert_columns,
                    "insert_values": insert_values,
                    "read": read,
                    "expected": expected,
                    "identity": identity,
                }
            )
    observation = livy_session.run(_observation_program(requests)).payload
    for qualified, (profile, allocation) in allocations.items():
        name = next(case["name"] for case in requests if case["qualified"] == qualified)
        assert (
            verify_delta_profile_v1(profile, observation[name]["snapshot"])
            == allocation
        )
    print("Delta protocol observation: " + json.dumps(observation, sort_keys=True))
    return results, observation


def _observation_program(requests):
    return (
        "import json, sys, importlib.metadata\n"
        "from pyspark.sql.functions import input_file_name\n"
        "_out = {'runtime': {'python': sys.version, 'spark': spark.version, 'deltalake': importlib.metadata.version('deltalake')}}\n"
        f"for _case in {requests!r}:\n"
        "    _q = _case['qualified']\n"
        "    _detail = spark.sql('DESCRIBE DETAIL ' + _q).collect()[0].asDict()\n"
        "    _lines = spark.read.text(_detail['location'] + '/_delta_log/*.json').withColumn('_file', input_file_name()).orderBy('_file').collect()\n"
        "    _protocol = None\n"
        "    _metadata = None\n"
        "    _version = max(int(line['_file'].rsplit('/', 1)[-1].split('.')[0]) for line in _lines)\n"
        "    for _line in _lines:\n"
        "        _row = json.loads(_line.value)\n"
        "        if 'protocol' in _row: _protocol = _row['protocol']\n"
        "        if 'metaData' in _row: _metadata = _row['metaData']\n"
        "    spark.sql('INSERT INTO ' + _q + ' (' + _case['insert_columns'] + ') SELECT ' + _case['insert_values']).collect()\n"
        "    _values = [r.asDict() for r in spark.sql('SELECT ' + _case['read'] + ' AS V' + (', Id' if _case['identity'] else '') + ' FROM ' + _q).collect()]\n"
        "    _out[_case['name']] = {'protocol': _protocol, 'snapshot': {'version': _version, 'protocol': _protocol, 'metaData': _metadata}, 'schema': json.loads(_metadata['schemaString']), 'dtype': spark.table(_q).schema['Value'].dataType.simpleString(), 'rows': _values, 'expected': _case['expected']}\n"
        "emit(_out)\n"
    )


@weaver_test(remote=True)
@pytest.mark.parametrize("route", ("direct", "spark"))
@pytest.mark.parametrize("case", [entry[0] for entry in CASES])
def test_runtime_table_protocol_features_and_readback_match_the_declaration(
    route, case, protocol_estate
):
    results, observation = protocol_estate
    name = f"{route}_{case}"
    result = results[name]
    seen = observation[name]
    expected_floor = (2, 5) if case == "explicit_scalar" else (3, 7)
    assert (
        seen["protocol"]["minReaderVersion"],
        seen["protocol"]["minWriterVersion"],
    ) == expected_floor
    reader = set(seen["protocol"].get("readerFeatures", ()))
    writer = set(seen["protocol"].get("writerFeatures", ()))
    assert ("timestampNtz" in reader) == (case in {"timestamp_ntz", "identity_ntz"})
    assert ("timestampNtz" in writer) == (case in {"timestamp_ntz", "identity_ntz"})
    assert ("variantType" in reader) == (case == "variant")
    assert ("variantType" in writer) == (case == "variant")
    assert ("identityColumns" in writer) == (case == "identity_ntz")
    assert "generatedColumns" not in writer
    assert seen["dtype"] == dict((entry[0], entry[1].lower()) for entry in CASES)[case]
    expected_row = {"V": seen["expected"]}
    if case == "identity_ntz":
        expected_row["Id"] = 1
    assert seen["rows"] == [expected_row]
    assert result.status == "succeeded"
    assert observation["runtime"]["deltalake"] == "1.6.6"
