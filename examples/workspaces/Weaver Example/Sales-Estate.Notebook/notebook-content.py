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
# META   }
# META }

# MARKDOWN ********************

# ## Import weaver and estate definition

# CELL ********************

import weaver

from pathlib import Path

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
        "Lakehouse/Weaver",
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

from weaver.workspaces import FabricWorkspace


workspace = FabricWorkspace(
    workspace="Weaver Example",
    weaver_lakehouse="Weaver",
    environment="weaver",
)

build_result = weaver.build(
    repository,
    workspace=workspace,    ## TODO this should just take weaver_lakehouse='Weaver'
    bind=[
        "Lakehouse/Sales=Lakehouse/Sales",
        "Warehouse/Reporting=Warehouse/Reporting",
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
    workspace=workspace,
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

# ## Or run one primitive on its own
#
# Every deployed object is independently runnable — no orchestrator, no
# catalogue, no bundle. `Sales__OrderSummary` is the generated form of the
# authored `Sales.OrderSummary.sql`, and nothing about calling it differs.

# CELL ********************

import sys

from weaver import lakehouse_for
from weaver.resolution import resolver_for
from weaver.targets import ItemRef

destination = lakehouse_for(resolver_for(workspace), ItemRef("Sales"))
sys.path.insert(0, f"{destination.files_root()}/_/Load")

from Sales__OrderSummary import Sales__OrderSummary

print(Sales__OrderSummary(spark, lakehouse=destination).load().as_row())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
