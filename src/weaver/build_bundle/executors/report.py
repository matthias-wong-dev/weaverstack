import json

from ...errors import InstallError
from ...report_definition import decode_report, verify_report


class ReportDefinitionExecutor:
    name = "report_definition"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        if spec["target_id"] != context.target.bound.id:
            raise InstallError("Report deployment requires its bound target")
        decode_report(spec["definition"])
        context.report_item(context.target.bound).update_definition(spec["definition"])


class ReportReadbackExecutor:
    name = "report_readback"

    def execute(self, action, payload, context):
        spec = json.loads(payload)
        if spec["target_id"] != context.target.bound.id:
            raise InstallError("Report readback requires its bound target")
        observed = context.report_item(context.target.bound).get_definition()
        verify_report(spec["definition"], observed)
        return {"report_definition": observed}
