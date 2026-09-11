import { expect, test } from "bun:test";
import { Context, Effect, Layer } from "effect";
import * as Schema from "effect/Schema";
import {
  FlowEngineService,
  InMemoryFlowPersistence,
  injectNode,
  layerFlowEngine,
} from "../src/index.ts";
import { defineNode } from "../src/declaration.ts";
import type { FlowLoadError } from "../src/engine.ts";

// A third-party host supplies a long-lived resource (e.g. an in-memory counter)
// to Nodes as Effect Layers. Node Declarations access it via ctx.service(key).
const Counter = Context.Service<{ next: () => number }>("Counter");

const makeCounterLayer = () => {
  let count = 0;
  return Layer.succeed(Counter, { next: () => ++count });
};

const greeterNode = defineNode(
  "greeter",
  Schema.Struct({ greeting: Schema.NonEmptyString }),
  (ctx) =>
    Effect.gen(function* () {
      const counter = yield* ctx.service(Counter);
      ctx.emit(`${ctx.config.greeting} #${counter.next()}`);
    }),
);

const greeterFlow = () => ({
  flowVersion: "1" as const,
  nodes: [
    { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { payload: "red" } },
    { id: "g1", type: "greeter", position: { x: 0, y: 40 }, config: { greeting: "hola" } },
    { id: "g2", type: "greeter", position: { x: 0, y: 80 }, config: { greeting: "bonjour" } },
  ],
  wires: [
    { source: "src", target: "g1" },
    { source: "g1", target: "g2" },
  ],
});

type EngineShape = typeof FlowEngineService.Service;
type RunRecord = { outputs: { nodeId: string; emitted: { port: string; payload: unknown }[] }[] };

const engineLayer = (resources?: Layer.Layer<any>) =>
  layerFlowEngine({
    adapter: InMemoryFlowPersistence(),
    declarations: [injectNode, greeterNode],
    resources,
  });

const runIn = <A>(
  resources: Layer.Layer<any>,
  effect: (engine: EngineShape) => Effect.Effect<A, FlowLoadError, never>,
) =>
  Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const engine = yield* FlowEngineService;
        return yield* effect(engine as EngineShape);
      }),
      engineLayer(resources),
    ),
  );

const loadError = (flow: unknown): Promise<FlowLoadError> =>
  Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const engine = yield* FlowEngineService;
        return yield* Effect.flip(engine.loadFlow(flow));
      }),
      engineLayer(makeCounterLayer()),
    ),
  );

test("custom node declaration runs in a flow and reaches the host Layer resource", async () => {
  const record = (await runIn(makeCounterLayer(), (engine) =>
    Effect.gen(function* () {
      const flow = yield* engine.loadFlow(greeterFlow());
      return (yield* engine.startRun(flow)) as unknown as RunRecord;
    }),
  )) as unknown as RunRecord;

  const g1 = record.outputs.filter((o) => o.nodeId === "g1");
  const g2 = record.outputs.filter((o) => o.nodeId === "g2");
  expect(g1).toHaveLength(1);
  expect(g2).toHaveLength(1);
  // the resource is long-lived: shared state across node invocations in the run
  expect(g1[0]!.emitted[0]).toEqual({ port: "0", payload: "hola #1" });
  expect(g2[0]!.emitted[0]).toEqual({ port: "0", payload: "bonjour #2" });
});

test("resource layer is built once per engine, not per run", async () => {
  const record = (await runIn(makeCounterLayer(), (engine) =>
    Effect.gen(function* () {
      const first = yield* engine.loadFlow(greeterFlow());
      yield* engine.startRun(first);
      const second = yield* engine.loadFlow(greeterFlow());
      return (yield* engine.startRun(second)) as unknown as RunRecord;
    }),
  )) as unknown as RunRecord;

  const g1 = record.outputs.filter((o) => o.nodeId === "g1");
  // run 2 continues where run 1 left off (2 increments in run 1, then 3)
  expect(g1[0]!.emitted[0]).toEqual({ port: "0", payload: "hola #3" });
});

test("invalid node config fails load naming the node and the field", async () => {
  const error = (await loadError({
    flowVersion: "1",
    nodes: [{ id: "g1", type: "greeter", position: { x: 0, y: 0 }, config: { greeting: "" } }],
    wires: [],
  })) as unknown as Record<string, unknown>;
  expect(error._tag).toBe("InvalidNodeConfigError");
  expect(error.nodeId).toBe("g1");
  expect(error.field).toBe("greeting");
});

test("missing config field fails load naming the node and the field", async () => {
  const error = (await loadError({
    flowVersion: "1",
    nodes: [{ id: "g1", type: "greeter", position: { x: 0, y: 0 }, config: {} }],
    wires: [],
  })) as unknown as Record<string, unknown>;
  expect(error._tag).toBe("InvalidNodeConfigError");
  expect(error.nodeId).toBe("g1");
  expect(error.field).toBe("greeting");
});

test("unregistered node type still rejected through the same registration surface", async () => {
  const error = (await loadError({
    flowVersion: "1",
    nodes: [{ id: "n1", type: "unregistered", position: { x: 0, y: 0 }, config: {} }],
    wires: [],
  })) as unknown as Record<string, unknown>;
  expect(error._tag).toBe("UnknownNodeDeclarationError");
  expect(error.nodeType).toBe("unregistered");
});
