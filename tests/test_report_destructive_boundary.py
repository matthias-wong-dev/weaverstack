from dataclasses import replace

import pytest
from support.weaver_test import weaver_test
from test_report_build_cycle import prepared_project
from test_semantic_model_build_cycle import ITEM

import weaver
from weaver.errors import BuildError, CommandError
from weaver.mutation.models import PhysicalScope
from weaver.mutation.validation import validate_mutation_plan


@weaver_test()
def test_naming_a_report_to_wipe_points_to_unbind_before_any_service_call(tmp_path):
    _, session, *_ = prepared_project(tmp_path)
    with pytest.raises(CommandError, match="weaver unbind Report/Executive_Dev"):
        weaver.plan_wipe("Report/Executive_Dev", session=session)
    assert not session.resolver().client.requested and not session.tsql


@weaver_test()
def test_shared_mutation_validation_refuses_destructive_report_scope(tmp_path):
    root, session, *_ = prepared_project(tmp_path)
    plans = []
    execute = session.execute_mutation

    def capture(plan, payloads=None, **options):
        plans.append(plan)
        return execute(plan, payloads, **options)

    session.execute_mutation = capture
    assert weaver.build(
        root,
        items=[
            f"{ITEM}=SemanticModel/Reporting_Dev",
            "Report/Executive=Report/Executive_Dev",
        ],
        session=session,
    ).succeeded
    plan = plans[0]
    sequences = []
    for sequence in plan.sequences:
        batches = []
        for batch in sequence.batches:
            actions = tuple(
                replace(
                    a,
                    writes=(PhysicalScope(a.target_id, ""),),
                    destructive_scopes=(PhysicalScope(a.target_id, ""),),
                )
                if a.executor == "report_definition"
                else a
                for a in batch.actions
            )
            batches.append(replace(batch, actions=actions))
        sequences.append(replace(sequence, batches=tuple(batches)))
    with pytest.raises(BuildError, match="Report.*destructive"):
        validate_mutation_plan(replace(plan, bundle_id="", sequences=tuple(sequences)))
