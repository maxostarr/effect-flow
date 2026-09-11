import { expect, test } from "bun:test";
import { Effect, Exit } from "effect";
import {
  FlowEngineService,
  InMemoryFlowPersistence,
  injectNode,
  layerFlowEngine,
  mapNode,
  debugNode,
} from "../src/index.ts";
import type { FlowLoadError } from "../src/engine.ts";

const run = (
  effect: (
    engine: typeof FlowEngineService.Service,
  ) => Effect.Effect<unknown, FlowLoadError, never>,
) =>
  Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const engine = yield* FlowEngineService;
        return (yield* effect(engine)) as void;
      }),
      layerFlowEngine({
        adapter: InMemoryFlowPersistence(),
        declarations: [injectNode, mapNode, debugNode],
      }),
    ),
  );

const sampleFlow = {
  flowVersion: "1",
  metadata: { name: "demo" },
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 40 }, config: { payload: 21 } },
    { id: "n2", type: "map", position: { x: 0, y: 80 }, config: { mult: 2 } },
    { id: "n3", type: "debug", position: { x: 0, y: 120 }, config: {} },
  ],
  wires: [
    { source: "n1", target: "n2" },
    { source: "n2", target: "n3" },
  ],
};

test("loadFlow honors flowVersion, metadata, positions", () =>
  run((engine) =>
    Effect.gen(function* () {
      const loaded = yield* engine.loadFlow(sampleFlow);
      expect(loaded.flow.flowVersion).toBe("1");
      expect(loaded.flow.metadata?.name).toBe("demo");
      expect(loaded.nodes.get("n1")?.node.position).toEqual({ x: 0, y: 40 });
    }),
  ));

test("loadFlow rejects unknown flowVersion", async () => {
  const result = await run((engine) =>
    Effect.exit(engine.loadFlow({ ...sampleFlow, flowVersion: "2" })),
  );
  expect(Exit.isFailure(result as never)).toBe(true);
});

test("inject starts a Run; map and debug reachable via wires", () =>
  run((engine) =>
    Effect.gen(function* () {
      const loaded = yield* engine.loadFlow(sampleFlow);
      const record = yield* engine.startRun(loaded);

      const mapped = record.outputs.filter((out) => out.nodeId === "n2");
      expect(mapped.length).toBe(1);
      expect(mapped[0]!.message.body).toBe(21);
      expect(mapped[0]!.emitted[0]).toEqual({ port: "0", payload: 42 });

      const debug = record.outputs.filter((out) => out.nodeId === "n3");
      expect(debug.length).toBe(1);
      expect(debug[0]!.message.body).toBe(42);
      expect(debug[0]!.emitted[0]).toEqual({ port: "0", payload: { observedBy: "n3", body: 42 } });
    }),
  ));

test("loadFlow fails on unknown node declaration", async () => {
  const result = await run((engine) =>
    Effect.exit(
      engine.loadFlow({
        flowVersion: "1",
        nodes: [{ id: "n1", type: "nosuch", position: { x: 0, y: 0 }, config: {} }],
        wires: [],
      }),
    ),
  );
  expect(Exit.isFailure(result as never)).toBe(true);
});

test("persistence adapter records run outputs behind in-memory implementation", () =>
  run((engine) =>
    Effect.gen(function* () {
      const loaded = yield* engine.loadFlow(sampleFlow);
      const record = yield* engine.startRun(loaded);
      const stored = yield* engine.adapter.getRun(record.runId);
      expect(stored._tag).toBe("Some");
      expect((stored as unknown as { value: { outputs: unknown[] } }).value.outputs.length).toBe(3);
    }),
  ));
