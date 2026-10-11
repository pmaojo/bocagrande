// The pipeline model shared with the Python side (bocagrande/pipeline/model.py), and the conversions to and from
// the graph that React Flow draws.
import type { Edge as FlowEdge, Node as FlowNode } from "@xyflow/react";

export interface PipelineNode {
  id: string;
  type: string;
  config: Record<string, unknown>;
  label?: string;
  position?: { x: number; y: number };
}
export interface PipelineEdge {
  source: string;
  target: string;
  handle?: string | null;
}
export interface Pipeline {
  name: string;
  description?: string;
  parameters: Record<string, string>;
  nodes: PipelineNode[];
  edges: PipelineEdge[];
}
export interface FieldSpec {
  name: string;
  kind: "text" | "number" | "bool" | "select" | "json";
  required: boolean;
  help: string;
  options?: string[];
}
export interface NodeTypeSpec {
  type: string;
  label: string;
  inputs?: string[];
  fields: FieldSpec[];
}
export interface NodeData extends Record<string, unknown> {
  type: string;
  config: Record<string, unknown>;
  label: string;
  inputs: string[];
}

export const kindOf = (type: string): string => type.split(".")[0];

export const emptyPipeline = (): Pipeline => ({ name: "new-pipeline", parameters: {}, nodes: [], edges: [] });

export function toFlow(pipeline: Pipeline, specs: NodeTypeSpec[]): { nodes: FlowNode<NodeData>[]; edges: FlowEdge[] } {
  const inputs = (type: string) => specs.find((s) => s.type === type)?.inputs ?? [];
  const nodes = pipeline.nodes.map((n, index) => ({
    id: n.id,
    type: "step",
    position: n.position ?? { x: 40 + (index % 4) * 240, y: 40 + Math.floor(index / 4) * 140 },
    data: { type: n.type, config: n.config ?? {}, label: n.label ?? n.id, inputs: inputs(n.type) },
  }));
  const edges = pipeline.edges.map((e, index) => ({
    id: `e${index}-${e.source}-${e.target}`,
    source: e.source,
    target: e.target,
    targetHandle: e.handle ?? undefined,
  }));
  return { nodes, edges };
}

export function fromFlow(base: Pipeline, nodes: FlowNode<NodeData>[], edges: FlowEdge[]): Pipeline {
  return {
    ...base,
    nodes: nodes.map((n) => ({
      id: n.id,
      type: n.data.type,
      config: n.data.config,
      label: n.data.label,
      position: { x: Math.round(n.position.x), y: Math.round(n.position.y) },
    })),
    edges: edges.map((e) => ({ source: e.source, target: e.target, handle: e.targetHandle ?? null })),
  };
}

export function uniqueId(type: string, taken: string[]): string {
  const base = type.split(".").pop() ?? "node";
  let candidate = base;
  for (let n = 2; taken.includes(candidate); n += 1) candidate = `${base}_${n}`;
  return candidate;
}

/** Parse the text of a JSON field; returns the value or an error message. */
export function parseJson(text: string): { value?: unknown; error?: string } {
  if (text.trim() === "") return { value: undefined };
  try {
    return { value: JSON.parse(text) };
  } catch (error) {
    return { error: (error as Error).message };
  }
}
