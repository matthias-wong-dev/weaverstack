from ..errors import ConfigError
from .client import FabricError


def validate_bound_report(item):
    import re

    from .resources import Item

    if not isinstance(item, Item) or item.type != "Report":
        raise ConfigError("A bound Report must name a typed Report item")
    for label, value in (("workspace ID", item.workspace_id), ("item ID", item.id)):
        if not isinstance(value, str) or not re.fullmatch(
            r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value
        ):
            raise ConfigError(f"Report/{item.name} has invalid {label} {value!r}")


class ReportClient:
    def __init__(self, workspace_id, report_id, *, fabric, power_bi_factory=None):
        self.workspace_id = workspace_id
        self.report_id = report_id
        self.fabric = fabric
        self._power_bi_factory = power_bi_factory
        self._power_bi = None
        self.item_path = f"workspaces/{workspace_id}/reports/{report_id}"

    def get_binding(self):
        if self._power_bi is None:
            if self._power_bi_factory is None:
                raise ConfigError(
                    "Report binding readback needs the Session's Power BI client"
                )
            self._power_bi = self._power_bi_factory()
        return self._power_bi.get_json(
            f"groups/{self.workspace_id}/reports/{self.report_id}"
        )

    def get_definition(self, *, timeout=900):
        response = self.fabric.request(
            "POST", f"{self.item_path}/getDefinition", expected=(200, 202)
        )
        if response.status_code == 202:
            self.fabric.wait_for_operation(response, timeout=timeout)
            operation = response.headers.get("x-ms-operation-id")
            if not operation:
                raise FabricError("Report definition has no operation ID")
            body = self.fabric.get_json(f"operations/{operation}/result")
        else:
            body = response.json()
        return body["definition"]

    def update_definition(self, definition, *, timeout=900):
        response = self.fabric.request(
            "POST",
            f"{self.item_path}/updateDefinition?updateMetadata=false",
            payload={"definition": definition},
            expected=(200, 202),
            retry_transient=False,
        )
        return self.fabric.wait_for_operation(response, timeout=timeout)
