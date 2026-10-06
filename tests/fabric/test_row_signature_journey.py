"""A real incremental load acting on the adversarial corpus, in both engines.

``test_row_signature_primitive.py`` proves each engine signs the corpus apart.
This proves the production path uses those signatures: a build installs one
incremental keyed Table per engine, both reading the whole corpus from ``dbo``
with a second relation claiming deletions, and three loads follow:

.. code-block:: text

    first       every case inserted, distinct rows signed apart
    changed     each change in ``signature_corpus.CHANGES`` updated, two cases
                inserted and two deleted; a value rewritten as an equal one and
                every untouched row keep their signature and update time
    unchanged   nothing inserted, updated or deleted, and nothing rewritten

Every load reads every case, so each row is compared with its stored signature
on every load and nothing is decided by a bookmark.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from support import signature_corpus as corpus
from support.weaver_test import weaver_test

import weaver
from weaver.declaration.metadata import (
    AUDIT_UPDATE,
    PYTHON,
    SQL,
    audit_column_name,
    signature_column_name,
)

SCHEMA = "Signature"
OBJECT = "Corpus"
LAKE = "Lakehouse/SignatureLake"
HOUSE = "Warehouse/SignatureHouse"
SOURCE = "SignatureSource"
RETIRED = "SignatureRetired"


def _header(engine: str) -> str:
    schema = "\n".join(
        f"  {column.name}: {column.target(engine)}" for column in corpus.columns(engine)
    )
    key = "string" if engine == corpus.LAKEHOUSE else "varchar(64)"
    return f"""Table ID: {SCHEMA}.{OBJECT}

Description: Rows built to break a row signature.

Lineage: The signature corpus in dbo.

Dependencies: []

Primary key: {corpus.KEY}

Incremental: true

Schema:
  {corpus.KEY}: {key}
{schema}"""


def write_repository(root: Path) -> Path:
    for kind, item in (("Lakehouse", LAKE), ("Warehouse", HOUSE)):
        schema = root / kind / item.split("/")[1] / "schemas" / f"{SCHEMA}.yml"
        schema.parent.mkdir(parents=True)
        schema.write_text(
            f"Schema ID: {SCHEMA}\nDescription: The signature corpus.\n",
            encoding="utf-8",
        )
    tables = root / "Lakehouse" / LAKE.split("/")[1] / "Tables"
    tables.mkdir()
    (tables / f"{SCHEMA}__{OBJECT}.py").write_text(
        f'"""\n{_header(corpus.LAKEHOUSE)}\n"""\n\n'
        "from weaver import Table\n\n\n"
        f"class {SCHEMA}__{OBJECT}(Table):\n"
        "    def read(self):\n"
        f'        rows = self.spark.table(self.lakehouse.qualify("dbo", "{SOURCE}"))\n'
        "        retired = self.spark.table(\n"
        f'            self.lakehouse.qualify("dbo", "{RETIRED}")\n'
        "        )\n"
        "        return rows, retired\n",
        encoding="utf-8",
    )
    (root / "Warehouse" / HOUSE.split("/")[1] / f"{SCHEMA}.{OBJECT}.sql").write_text(
        f"/*\n{_header(corpus.WAREHOUSE)}\n*/\n"
        f"select {corpus.tsql_staged_select()}\nfrom [dbo].[{SOURCE}];\n\n"
        f"select [{corpus.KEY}] from [dbo].[{RETIRED}];\n",
        encoding="utf-8",
    )
    return root


@dataclass(frozen=True)
class Snapshot:
    """One engine's table after one load: each case's signature and update time."""

    result: object
    signatures: dict
    updated: dict


class Estate:
    def __init__(self, *, session, livy, workspace, lakehouse, warehouse):
        self.session = session
        self.livy = livy
        self.lakehouse = lakehouse
        self.warehouse = warehouse
        self.physical = {
            LAKE: f"Lakehouse/{lakehouse}",
            HOUSE: f"Warehouse/{warehouse.item.name}",
        }
        self.dbo = f"`{workspace.workspace}`.`{lakehouse}`.`dbo`"
        self.table = f"`{workspace.workspace}`.`{lakehouse}`.`{SCHEMA}`.`{OBJECT}`"

    # --- the source ------------------------------------------------------------

    def seed(self, lake, house, retired=()) -> None:
        """Replace both engines' source and deletion claims."""

        relation = corpus.spark_rows(lake)
        claims = ", ".join(f"('{case_id}')" for case_id in retired)
        body = (
            f"spark.sql('CREATE SCHEMA IF NOT EXISTS {self.dbo}')\n"
            f"spark.sql('''CREATE OR REPLACE TABLE {self.dbo}.`{SOURCE}` "
            f"USING delta AS {relation}''')\n"
            f"spark.sql('CREATE OR REPLACE TABLE {self.dbo}.`{RETIRED}` "
            f"(`{corpus.KEY}` STRING) USING delta')\n"
        )
        if claims:
            body += f'spark.sql("INSERT INTO {self.dbo}.`{RETIRED}` VALUES {claims}")\n'
        self.livy.run(body + "emit({'seeded': True})\n")

        executor = self.warehouse.executor
        script = (
            f"drop table if exists [dbo].[{SOURCE}];\n"
            f"drop table if exists [dbo].[{RETIRED}];\n"
            f"create table [dbo].[{SOURCE}] ({corpus.tsql_source_columns()});\n"
            f"create table [dbo].[{RETIRED}] ([{corpus.KEY}] varchar(64) not null);\n"
            f"insert into [dbo].[{SOURCE}] values\n"
            f"{corpus.tsql_rows(house)};\n"
        )
        if claims:
            script += f"insert into [dbo].[{RETIRED}] values {claims};\n"
        executor.execute_script(script)

    # --- operations ------------------------------------------------------------

    def build(self, repository: Path) -> None:
        built = weaver.build(
            str(repository),
            items=[f"{item}={name}" for item, name in self.physical.items()],
            session=self.session,
        )
        assert built.succeeded, built

    def load(self) -> dict[str, Snapshot]:
        report = weaver.load([LAKE, HOUSE], session=self.session)
        assert report.succeeded, report
        lake = report.by_node[f"load:{self.physical[LAKE]}/Tables/{SCHEMA}.{OBJECT}"]
        house = report.by_node[f"load:{self.physical[HOUSE]}/{SCHEMA}.{OBJECT}"]
        return {
            corpus.LAKEHOUSE: self._lakehouse(lake.result),
            corpus.WAREHOUSE: self._warehouse(house.result),
        }

    def _lakehouse(self, result) -> Snapshot:
        signature = signature_column_name(PYTHON)
        updated = audit_column_name(AUDIT_UPDATE, PYTHON)
        rows = self.livy.run(
            f"rows = spark.sql('''SELECT `{corpus.KEY}`, `{signature}`, "
            f"CAST(`{updated}` AS STRING) FROM {self.table}''').collect()\n"
            "emit([list(row) for row in rows])\n"
        ).payload
        return Snapshot(
            result,
            {row[0]: row[1] for row in rows},
            {row[0]: row[2] for row in rows},
        )

    def _warehouse(self, result) -> Snapshot:
        signature = signature_column_name(SQL)
        updated = audit_column_name(AUDIT_UPDATE, SQL)
        rows = self.warehouse.executor.query(
            f"select [{corpus.KEY}] as id, [{signature}] as signature, "
            f"convert(varchar(27), [{updated}], 126) as updated "
            f"from [{SCHEMA}].[{OBJECT}]"
        )
        return Snapshot(
            result,
            {row["id"]: bytes(row["signature"]) for row in rows},
            {row["id"]: row["updated"] for row in rows},
        )


@pytest.fixture(scope="module")
def journey(
    tmp_path_factory,
    weaver_session,
    livy_session,
    fabric_workspace,
    fabric_target_lakehouse,
    fabric_empty_lakehouse,
    fabric_lakehouse_cleanup,
    clean_disposable_warehouse,
    fabric_initialise_catalogue,
):
    """The three loads, each captured as it finished."""

    fabric_lakehouse_cleanup(fabric_target_lakehouse.name)
    fabric_empty_lakehouse(fabric_target_lakehouse.name)
    fabric_initialise_catalogue()
    estate = Estate(
        session=weaver_session,
        livy=livy_session,
        workspace=fabric_workspace,
        lakehouse=fabric_target_lakehouse.name,
        warehouse=clean_disposable_warehouse,
    )
    estate.seed(corpus.cases(corpus.LAKEHOUSE), corpus.cases(corpus.WAREHOUSE))
    estate.build(write_repository(tmp_path_factory.mktemp("signature")))
    first = estate.load()
    estate.seed(
        corpus.changed_cases(corpus.LAKEHOUSE),
        corpus.changed_cases(corpus.WAREHOUSE),
        retired=corpus.DELETES,
    )
    changed = estate.load()
    unchanged = estate.load()
    return {"first": first, "changed": changed, "unchanged": unchanged}


ENGINES = pytest.mark.parametrize("engine", [corpus.LAKEHOUSE, corpus.WAREHOUSE])


def _counts(result) -> tuple:
    return (
        result.rows_read,
        result.rows_inserted,
        result.rows_updated,
        result.rows_deleted,
        result.rows_rejected,
    )


@weaver_test(integration=True)
@ENGINES
def test_the_first_load_inserts_every_case(journey, engine):
    first = journey["first"][engine]
    total = len(corpus.cases(engine))

    assert _counts(first.result) == (total, total, 0, 0, 0)
    assert set(first.signatures) == {case.id for case in corpus.cases(engine)}


@weaver_test(integration=True)
@ENGINES
def test_the_first_load_signs_distinct_rows_apart(journey, engine):
    signatures = journey["first"][engine].signatures

    assert corpus.collisions(signatures, engine) == []
    assert corpus.partition(signatures) == corpus.expected_partition(engine)


@weaver_test(integration=True)
@ENGINES
def test_a_load_updates_each_changed_row_and_no_other(journey, engine):
    first = journey["first"][engine]
    changed = journey["changed"][engine]
    expected = corpus.changed_ids(engine)
    kept = set(first.signatures) - set(corpus.DELETES)

    updated = {
        case_id
        for case_id in kept
        if changed.updated[case_id] != first.updated[case_id]
    }
    resigned = {
        case_id
        for case_id in kept
        if changed.signatures[case_id] != first.signatures[case_id]
    }

    assert _counts(changed.result) == (
        len(corpus.changed_cases(engine)),
        len([case for case in corpus.INSERTS if case.applies_to(engine)]),
        len(expected),
        len(corpus.DELETES),
        0,
    )
    assert sorted(updated) == sorted(expected)
    assert sorted(resigned) == sorted(expected)
    assert set(changed.signatures) == {case.id for case in corpus.changed_cases(engine)}


@weaver_test(integration=True)
@ENGINES
def test_the_changed_rows_are_still_signed_apart(journey, engine):
    signatures = journey["changed"][engine].signatures
    changed = corpus.changed_cases(engine)

    assert corpus.collisions(signatures, engine, changed) == []
    assert corpus.partition(signatures) == corpus.expected_partition(engine, changed)


@weaver_test(integration=True)
@ENGINES
def test_an_unchanged_load_changes_nothing(journey, engine):
    changed = journey["changed"][engine]
    unchanged = journey["unchanged"][engine]

    assert _counts(unchanged.result) == (len(corpus.changed_cases(engine)), 0, 0, 0, 0)
    assert unchanged.signatures == changed.signatures
    assert unchanged.updated == changed.updated
