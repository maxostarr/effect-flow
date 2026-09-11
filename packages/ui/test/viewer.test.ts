import { describe, expect, test } from "bun:test";
import { toReactFlow } from "../src/viewer.tsx";
import type { FlowSchema } from "@effect-flow/core";

const flow: FlowSchema = {
  flowVersion: "1",
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: {} },
    { id: "n2", type: "map", position: { x: 10, y: 20 }, config: { mult: 2 } },
  ],
  wires: [{ source: "n1", target: "n2" }],
};

describe("toReactFlow", () => {
  test("maps nodes at stored positions", () => {
    const { nodes } = toReactFlow(flow);
    expect(nodes).toHaveLength(2);
    expect(nodes[1]?.position).toEqual({ x: 10, y: 20 });
    expect(nodes[0]?.type).toBe("flowNode");
  });

  test("maps wires to source:port->target edges", () => {
    const { edges } = toReactFlow(flow);
    expect(edges).toEqual([{ id: "n1:0->n2", source: "n1", target: "n2" }]);
  });

  test("two wires to the same target render as distinct edges", () => {
    const twoWireFlow: FlowSchema = {
      flowVersion: "1",
      nodes: [
        { id: "a", type: "inject", position: { x: 0, y: 0 }, config: {} },
        { id: "b", type: "map", position: { x: 1, y: 0 }, config: { mult: 1 } },
      ],
      wires: [
        { source: "a", target: "b", port: "hot" },
        { source: "a", target: "b", port: "cold" },
      ],
    };
    const { edges } = toReactFlow(twoWireFlow);
    expect(edges.map((e) => e.id)).toEqual(["a:hot->b", "a:cold->b"]);
  });
});
