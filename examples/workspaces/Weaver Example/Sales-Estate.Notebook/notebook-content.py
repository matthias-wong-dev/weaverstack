# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "f2346c37-1d0a-9f65-447d-0af85c370d9f",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META }

# MARKDOWN ********************

# ## Import weaver and estate definition

# CELL ********************

from pathlib import Path

import weaver

repository = Path("builtin") / "repository"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Wipe existing estate

# CELL ********************

wipe_result = weaver.wipe(
    [
        "Lakehouse/Sales",
        "Warehouse/Reporting",
        "Warehouse/Weaver",
    ]
)

print(f"Removed {wipe_result.count} managed objects")

for report in wipe_result.reports:
    print(f"{report.target}: {report.count} removed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Build sales estate example

# CELL ********************


build_result = weaver.build(
    repository,
    catalogue="Warehouse/Weaver",
    bind=[
        "Lakehouse/Sales=Sales",
        "Warehouse/Reporting=Reporting",
    ],
)

if not build_result.succeeded:
    details = "\n".join(
        f"{error.action_id}: {error.error_type}: {error.message}"
        for error in build_result.errors
    )
    raise RuntimeError(f"Weaver build failed:\n{details}")

print(f"Build succeeded: {build_result.bundle_id}")

for item in build_result.items:
    print(f"Built: {item}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load the estate
#
# A build creates structure; a load puts rows in it. One call orchestrates
# both items in dependency order — the folder, the two Python tables, the
# Spark SQL table, the endpoint refresh that lets the Warehouse read across,
# and the Warehouse table that consumes it.

# CELL ********************

load_result = weaver.load(
    [
        "Lakehouse/Sales",
        "Warehouse/Reporting",
    ],
)

print(f"Load {load_result.status}")

for node in load_result.nodes:
    rows = "" if node.result is None else f"  (+{node.result.rows_inserted})"
    print(f"{node.status:<24} {node.node_id}{rows}")

print(f"Evidence: {load_result.task_log}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validate the estate
#
# A build creates structure and a load puts rows in it; neither says whether
# the rows that landed are the rows the estate declared they would be. A Test
# compares an expected relation with an actual one and passes when the
# symmetric difference is empty; an Assumption returns the rows that
# contradict it and passes when there are none.
#
# One call runs both kinds, across both engines — the Spark SQL Test and the
# Python Assumption in the Lakehouse, and the T-SQL Assumption compiled into a
# stored procedure in the Warehouse.
#
# A whole-target run reports **counts only**. Diagnostic rows may be large and
# may carry sensitive business data, so they are never transferred and never
# written to the task log.

# CELL ********************

test_result = weaver.test(
    [
        "Lakehouse/Sales",
        "Warehouse/Reporting",
    ],
)

print(f"Test {test_result.status}\n")

for node in test_result.nodes:
    result = node.result
    if hasattr(result, "violation_count"):
        found = f"{result.violation_count} violation(s)"
    else:
        found = f"{result.missing_count} missing, {result.unexpected_count} unexpected"
    print(f"{node.status:<10} {node.kind:<11} {node.logical_id}  ({found})")

totals = test_result.totals()
print(
    f"\n{totals['passed']} passed, {totals['failed']} failed, "
    f"{totals['invalid']} could not run"
)
print(f"Evidence: {test_result.task_log}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Name one, and see the rows
#
# Naming a single validation returns its diagnostic rows *as well as* its
# counts, from the same execution — a Test run twice would compare data that
# could have changed in between.
#
# Each row says which side it came from and carries `_weaver_sk`, which is the
# same for both sides of one changed entity when the Test declares a primary
# key. So a reader can tell *missing* from *unexpected* from *different*
# without Weaver having to decide which it was.

# CELL ********************

named = weaver.test(
    "Lakehouse/Sales",
    name="Sales.OrderSummaryReconciliation",
)

node = named.nodes[0]
print(f"{node.status}: {node.result.failure_count} discrepancy row(s)\n")

if node.diagnostics:
    display(spark.createDataFrame(list(node.diagnostics)))
else:
    print("The summary reconciles: nothing to show.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Or run one primitive on its own
#
# Every deployed object is independently runnable — no orchestrator, no
# catalogue, no bundle. `Sales__OrderSummary` is the generated form of the
# authored `Sales.OrderSummary.sql`, and nothing about calling it differs.

# CELL ********************

import sys

from weaver import current_workspace, lakehouse_for
from weaver.resolution import resolver_for
from weaver.targets import ItemRef

# Nothing names the workspace: a session already knows which one it is, and
# `current_workspace()` is that discovery. What *is* named is the Lakehouse —
# a destination is never inherited, even when there is only one.
destination = lakehouse_for(resolver_for(current_workspace()), ItemRef("Sales"))
sys.path.insert(0, f"{destination.files_root()}/_/Load")

from Sales__OrderSummary import Sales__OrderSummary

print(Sales__OrderSummary(spark, lakehouse=destination).load().as_row())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## And one validation on its own
#
# The same property, for validation. This opens no catalogue, invokes no
# orchestrator and writes no task log — it imports the class and calls it, and
# what comes back is a DataFrame you can look at.
#
# It is the loop a developer is actually in: write an Assumption, run it, fix
# it. `weaver.test(..., file=...)` does the same for a SQL validation that has
# not been built yet.

# CELL ********************

from assumptions.Sales__OrderCustomerExists import Sales__OrderCustomerExists

violations = Sales__OrderCustomerExists(spark, lakehouse=destination).read()

print(f"{violations.count()} order(s) name a customer that is not there")
display(violations)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
