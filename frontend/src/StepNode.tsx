import { Handle, Position, type NodeProps, type Node } from "@xyflow/react";
import { kindOf, type NodeData } from "./model";

export type StepFlowNode = Node<NodeData & { status?: string }, "step">;

// The kind class is prefixed on purpose: React Flow finds a node's handles with the selectors ".source" and
// ".target", so a plain "source" class on the node itself would be taken for a handle.
export function StepNode({ data, selected }: NodeProps<StepFlowNode>) {
  const kind = kindOf(data.type);
  const handles = data.inputs.length > 0 ? data.inputs : kind === "source" ? [] : [undefined];
  return (
    <div className={`step kind-${kind} ${selected ? "selected" : ""} ${data.status ? `run-${data.status}` : ""}`}>
      {handles.map((handle, index) => (
        <Handle
          key={handle ?? "in"}
          id={handle}
          type="target"
          position={Position.Left}
          style={{ top: handles.length > 1 ? `${((index + 1) * 100) / (handles.length + 1)}%` : "50%" }}
          title={handle}
        />
      ))}
      <strong>{data.label}</strong>
      <small>{data.type}</small>
      {data.inputs.length > 0 && <small>inputs: {data.inputs.join(" / ")}</small>}
      {kind !== "destination" && <Handle type="source" position={Position.Right} />}
    </div>
  );
}
