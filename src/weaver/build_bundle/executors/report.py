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
        report = context.report_item(context.target.bound)
        observed = report.get_definition()
        binding = spec.get("binding")
        verify_report(
            spec["definition"],
            observed,
            binding=binding,
            service_binding=report.get_binding() if binding is not None else None,
            report_name=context.target.bound.name,
        )
        return {"report_definition": observed}
