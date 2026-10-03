from ..errors import BuildError
from .models import PhysicalScope


class ScopeRules:
    def __init__(self, plan):
        self.targets = {target.id: target for target in plan.targets}
        self.workspace = plan.execution.workspace_id
        for value in (
            self.workspace,
            *(v for t in self.targets.values() for v in (t.item_id, t.workspace_id)),
        ):
            if value is not None and value != value.strip():
                raise BuildError(f"physical identity must be canonical: {value!r}")

    def check(self, scope):
        from .bundle import _check_relative
        from .serialization import require_string

        if not isinstance(scope, PhysicalScope):
            raise BuildError("physical scope must be a frozen PhysicalScope")
        require_string(scope.target_id, what="scope target")
        if scope.target_id not in self.targets:
            raise BuildError(f"unknown scope target {scope.target_id!r}")
        if not isinstance(scope.path, str):
            raise BuildError("scope path must be a string")
        if any(part != part.strip() for part in scope.path.split("/")):
            raise BuildError(f"scope path must be canonical: {scope.path!r}")
        if scope.path:
            _check_relative(scope.path, what="scope path")

    def workspace_id(self, target):
        if target.workspace_id is not None:
            return target.workspace_id
        if target.workspace_name is None:
            return self.workspace
        return None

    def covers(self, parent, child):
        left, right = self.targets[parent.target_id], self.targets[child.target_id]
        if (left.kind, left.item_id) != (right.kind, right.item_id):
            return False
        a, b = self.workspace_id(left), self.workspace_id(right)
        return (a is None or b is None or a == b) and (
            parent.path == ""
            or parent.path == child.path
            or child.path.startswith(parent.path + "/")
        )

    def overlaps(self, left, right):
        return self.covers(left, right) or self.covers(right, left)
