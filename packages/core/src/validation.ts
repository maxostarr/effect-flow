import { Effect, Graph, Option, Schema } from "effect";
import { isEntryNode, type FlowSchema, type WireSchema } from "./schema.ts";

const describeWire = (wire: WireSchema): string => `${wire.source} -> ${wire.target}`;

export class FlowCycleError extends Schema.TaggedError<FlowCycleError>()("FlowCycleError", {
  nodes: Schema.Array(Schema.String),
  wires: Schema.Array(Schema.String),
  message: Schema.String,
}) {}

export class DanglingWireError extends Schema.TaggedError<DanglingWireError>()(
  "DanglingWireError",
  {
    wire: Schema.String,
    missingNodeId: Schema.String,
    message: Schema.String,
  },
) {}

export class UnreachableNodesError extends Schema.TaggedError<UnreachableNodesError>()(
  "UnreachableNodesError",
  {
    nodes: Schema.Array(Schema.String),
    message: Schema.String,
  },
) {}

export type FlowTopologyError = FlowCycleError | DanglingWireError | UnreachableNodesError;

const buildFlowGraph = Graph.directed<string, string>;

export const validateTopology = (flow: FlowSchema): Effect.Effect<void, FlowTopologyError> =>
  Effect.gen(function* () {
    const nodeIds = new Set(flow.nodes.map((node) => node.id));

    for (const wire of flow.wires) {
      const missing = !nodeIds.has(wire.source)
        ? wire.source
        : !nodeIds.has(wire.target)
          ? wire.target
          : undefined;
      if (missing !== undefined) {
        return yield* Effect.fail(
          new DanglingWireError({
            wire: describeWire(wire),
            missingNodeId: missing,
            message: `Wire "${describeWire(wire)}" references nonexistent node "${missing}"`,
          }),
        );
      }
    }

    const idToIndex = new Map<string, Graph.NodeIndex>();
    const indexToId = new Map<Graph.NodeIndex, string>();
    const graph = buildFlowGraph((mutable) => {
      for (const node of flow.nodes) {
        idToIndex.set(node.id, Graph.addNode(mutable, node.id));
      }
      for (const wire of flow.wires) {
        Graph.addEdge(
          mutable,
          idToIndex.get(wire.source)!,
          idToIndex.get(wire.target)!,
          describeWire(wire),
        );
      }
    });

    for (const [index, id] of Array.from(Graph.entries(Graph.nodes(graph)))) {
      indexToId.set(index, id);
    }

    const cycle = Graph.findCycle(graph);
    if (Option.isSome(cycle)) {
      const nodes = cycle.value.path.map((index) => indexToId.get(index)!);
      const wires = cycle.value.edges.map((edgeIndex) => {
        const edge = Graph.getEdge(graph, edgeIndex);
        return Option.isSome(edge) ? edge.value.data : "";
      });
      return yield* Effect.fail(
        new FlowCycleError({
          nodes,
          wires,
          message: `Flow contains a cycle through nodes [${nodes.join(" -> ")}] via wires: ${wires.join(", ")}`,
        }),
      );
    }

    const entryIds = new Set(flow.nodes.filter(isEntryNode).map((node) => node.id));
    if (entryIds.size > 0) {
      const startIndices: Array<Graph.NodeIndex> = [];
      for (const entryId of entryIds) {
        const index = idToIndex.get(entryId);
        if (index !== undefined) startIndices.push(index);
      }
      const reached = new Set<Graph.NodeIndex>(
        Array.from(Graph.indices(Graph.bfs({ start: startIndices, direction: "outgoing" })(graph))),
      );
      const unreachable = [...indexToId.entries()]
        .filter(([index]) => !reached.has(index))
        .map(([, id]) => id);
      if (unreachable.length > 0) {
        return yield* Effect.fail(
          new UnreachableNodesError({
            nodes: unreachable,
            message: `Nodes unreachable from inject nodes: ${unreachable.join(", ")}`,
          }),
        );
      }
    }
  });
