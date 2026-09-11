import { expect, test } from "bun:test";
import { Effect } from "effect";
import { injectNode, mapNode, debugNode } from "@effect-flow/nodes-basic";
import { FlowEngineService, InMemoryFlowPersistence, layerFlowEngine } from "../src/index.ts";
import { DanglingWireError, FlowCycleError, UnreachableNodesError } from "../src/validation.ts";
import type { LoadedFlow } from "../src/engine.ts";

const load = (flow: unknown) =>
  Effect.gen(function* () {
    const engine = yield* FlowEngineService;
    return yield* engine.loadFlow(flow as never);
  }).pipe(
    Effect.provide(
      layerFlowEngine({
        adapter: InMemoryFlowPersistence(),
        declarations: [injectNode, mapNode, debugNode],
      }),
    ),
    Effect.result,
  );

type LoadError = {
  _tag: string;
  nodes?: Array<string>;
  wires?: Array<string>;
  wire?: string;
  missingNodeId?: string;
  message: string;
};

type LoadOutcome = { tag: "loaded"; loaded: LoadedFlow } | { tag: "error"; error: LoadError };

const runLoad = async (flow: unknown): Promise<LoadOutcome> => {
  const result = await Effect.runPromise(load(flow) as never);
  const { _tag, success, failure } = result as unknown as {
    _tag: "Success" | "Failure";
    success: LoadedFlow;
    failure: LoadError;
  };
  if (_tag === "Success") return { tag: "loaded", loaded: success };
  return { tag: "error", error: failure };
};

const expectLoadError = async (
  flow: unknown,
  errorClass: {
    new (...args: never): any;
  },
): Promise<LoadError> => {
  const outcome = await runLoad(flow);
  if (outcome.tag !== "error") throw new Error("expected load failure, got success");
  expect(outcome.error._tag).toBe(errorClass.name);
  return outcome.error;
};

const node = (
  id: string,
  type = "map",
  config: unknown = type === "inject" ? { body: 1 } : type === "debug" ? {} : { mult: 1 },
) => ({
  id,
  type,
  position: { x: 0, y: 0 },
  config,
});
const flow = (nodes: Array<unknown>, wires: Array<unknown>) => ({ flowVersion: "1", nodes, wires });

test("cyclic flow rejected at load naming involved wires", async () => {
  const error = await expectLoadError(
    flow(
      [node("a", "inject"), node("b"), node("c")],
      [
        { source: "a", target: "b" },
        { source: "b", target: "c" },
        { source: "c", target: "b" },
      ],
    ),
    FlowCycleError,
  );
  expect(error.wires).toEqual(expect.arrayContaining(["b -> c", "c -> b"]));
  expect(error.nodes).toEqual(expect.arrayContaining(["b", "c"]));
  expect(error.message).toContain("b -> c");
  expect(error.message).toContain("cycle");
});

test("dangling wire rejected at load", async () => {
  const error = await expectLoadError(
    flow([node("a", "inject")], [{ source: "a", target: "ghost" }]),
    DanglingWireError,
  );
  expect(error.missingNodeId).toBe("ghost");
  expect(error.message).toContain("ghost");
});

test("valid acyclic flow passes cleanly", async () => {
  const outcome = await runLoad(
    flow(
      [node("a", "inject"), node("b"), node("c", "debug")],
      [
        { source: "a", target: "b" },
        { source: "b", target: "c" },
      ],
    ),
  );
  expect(outcome.tag).toBe("loaded");
  if (outcome.tag !== "loaded") throw new Error("expected success");
  expect([...outcome.loaded.nodes.keys()].sort()).toEqual(["a", "b", "c"]);
});

test("all nodes reachable from inject pass validation", async () => {
  const outcome = await runLoad(
    flow([node("a", "inject"), node("b")], [{ source: "a", target: "b" }]),
  );
  expect(outcome.tag).toBe("loaded");
});

test("nodes unreachable from inject rejected", async () => {
  const error = await expectLoadError(
    flow(
      [node("a", "inject"), node("b"), node("d")],
      [
        { source: "a", target: "b" },
        { source: "d", target: "b" },
      ],
    ),
    UnreachableNodesError,
  );
  expect(error.nodes).toContain("d");
  expect(error.nodes).not.toContain("b");
  expect(error.message).toContain("unreachable");
});
