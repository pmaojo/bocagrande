import { describe, expect, it } from "vitest";
import { fromFlow, parseJson, toFlow, uniqueId, type NodeTypeSpec, type Pipeline } from "./model";

const specs: NodeTypeSpec[] = [{ type: "transform.merge_join", label: "Merge join", inputs: ["left", "right"], fields: [] }];
const pipeline: Pipeline = {
  name: "p",
  parameters: { P: "1" },
  nodes: [
    { id: "a", type: "source.file", config: { path: "a.csv" } },
    { id: "j", type: "transform.merge_join", config: {}, position: { x: 5, y: 6 } },
  ],
  edges: [{ source: "a", target: "j", handle: "left" }],
};

describe("model", () => {
  it("round-trips through the flow graph", () => {
    const { nodes, edges } = toFlow(pipeline, specs);
    expect(nodes[1].data.inputs).toEqual(["left", "right"]);
    expect(edges[0].targetHandle).toBe("left");
    const back = fromFlow(pipeline, nodes, edges);
    expect(back.edges).toEqual(pipeline.edges);
    expect(back.nodes[1].position).toEqual({ x: 5, y: 6 });
    expect(back.parameters).toEqual({ P: "1" });
  });
  it("makes unique ids", () => {
    expect(uniqueId("source.file", ["file", "file_2"])).toBe("file_3");
  });
  it("parses JSON fields and reports errors", () => {
    expect(parseJson('["a"]').value).toEqual(["a"]);
    expect(parseJson("").value).toBeUndefined();
    expect(parseJson("[").error).toBeTruthy();
  });
});
