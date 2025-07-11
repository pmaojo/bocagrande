"""Models used to represent Pentaho transformation components."""

from __future__ import annotations

from typing import List, Dict, Any
from collections import deque
from app.logger import get_logger

logger = get_logger(__name__)


class TransformationCycleError(Exception):
    """Raised when the transformation graph contains a cycle."""

    pass


class Connection:
    def __init__(
        self,
        name: str,
        db_type: str,
        host: str,
        db_name: str,
        port: str,
        user: str,
        password: str,
    ):
        self.name = name
        self.type = db_type
        self.host = host
        self.db_name = db_name
        self.port = port
        self.user = user
        self.password = password  # Consider secure handling for passwords

    def __repr__(self):
        return (
            f"Connection(name='{self.name}', type='{self.type}', db='{self.db_name}')"
        )


class Field:
    def __init__(
        self, name: str, data_type: str, length: int = -1, precision: int = -1
    ):
        self.name = name
        self.data_type = data_type
        self.length = length
        self.precision = precision

    def __repr__(self):
        return f"Field(name='{self.name}', type='{self.data_type}')"


class Step:
    def __init__(
        self,
        name: str,
        step_type: str,
        config: Dict[str, Any] = None,
        gui_location: Dict[str, int] = None,
    ):
        self.name = name
        self.type = step_type
        self.config = config if config else {}
        self.fields: List[Field] = []
        self.gui_location = gui_location if gui_location else {"x": 0, "y": 0}
        self.sql: str = ""
        self.target_schema: str = ""
        self.target_table: str = ""
        self.connection_name: str = ""

    def __repr__(self):
        return f"Step(name='{self.name}', type='{self.type}')"


class Hop:
    def __init__(self, from_step: str, to_step: str, enabled: bool = True):
        self.from_step = from_step
        self.to_step = to_step
        self.enabled = enabled

    def __repr__(self):
        return f"Hop(from='{self.from_step}', to='{self.to_step}')"


class TransformationModel:
    def __init__(self, name: str = "", description: str = "", directory: str = "/"):
        self.name = name
        self.description = description
        self.directory = directory
        self.connections: List[Connection] = []
        self.steps: List[Step] = []
        self.hops: List[Hop] = []
        self.parameters: Dict[str, str] = {}
        self.attributes: Dict[str, Any] = {}

    def add_connection(self, connection: Connection):
        self.connections.append(connection)

    def add_step(self, step: Step):
        self.steps.append(step)

    def add_hop(self, hop: Hop):
        self.hops.append(hop)

    def get_step_by_name(self, name: str) -> Step | None:
        for step in self.steps:
            if step.name == name:
                return step
        return None

    def get_connection_by_name(self, name: str) -> Connection | None:
        for conn in self.connections:
            if conn.name == name:
                return conn
        return None

    def get_execution_order(self) -> List[Step]:
        adj: Dict[str, List[str]] = {step.name: [] for step in self.steps}
        in_degree: Dict[str, int] = {step.name: 0 for step in self.steps}

        for hop in self.hops:
            if hop.enabled:
                adj[hop.from_step].append(hop.to_step)
                in_degree[hop.to_step] += 1

        queue: deque[str] = deque(
            [step_name for step_name, degree in in_degree.items() if degree == 0]
        )
        order: List[str] = []

        while queue:
            u = queue.popleft()
            order.append(u)
            for v_name in adj[u]:
                in_degree[v_name] -= 1
                if in_degree[v_name] == 0:
                    queue.append(v_name)

        step_by_name: Dict[str, Step] = {step.name: step for step in self.steps}

        connected_steps = {
            hop.from_step for hop in self.hops
        } | {hop.to_step for hop in self.hops}

        if len(order) != len(self.steps) or len(connected_steps) != len(self.steps):
            # Detect cycles or orphan steps and log a warning for better debugging
            logger.info(
                "Warning: Topological sort might be incomplete. Steps processed: "
                f"{len(order)}/{len(self.steps)}"
            )

        return [step_by_name[name] for name in order if name in step_by_name]

    def __repr__(self):
        return f"TransformationModel(name='{self.name}', steps={len(self.steps)}, hops={len(self.hops)})"
