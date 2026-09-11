import { expect, test } from "bun:test";
import { Effect, Exit } from "effect";
import * as Schema from "effect/Schema";
import {
  FlowEngineService,
  InMemoryFlowPersistence,
  injectNode,
  layerFlowEngine,
  mapNode,
  debugNode,
  backoffMs,
  DEAD_LETTER_PORT,
  defineNode,
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

class NetworkError extends Schema.TaggedError<NetworkError>()("NetworkError", {}) {}
const makeFlakyNode = (attemptCounter: { count: number }) =>
  defineNode("flaky", Schema.Struct({ failures: Schema.Number }), (ctx) =>
    Effect.suspend(() => {
      attemptCounter.count++;
      if (attemptCounter.count <= ctx.config.failures) {
        return Effect.fail(new NetworkError());
      }
      ctx.emit(`ok-${ctx.message.id}`);
      return Effect.void;
    }),
  );

const retryEngineLayer = (extra: ReturnType<typeof defineNode>[]) =>
  layerFlowEngine({
    adapter: InMemoryFlowPersistence(),
    declarations: [injectNode, debugNode, ...extra],
  });

const attemptRun = (declarations: ReturnType<typeof defineNode>[], flow: unknown) =>
  Effect.gen(function* () {
    const engine = yield* FlowEngineService;
    const loaded = yield* engine.loadFlow(flow);
    return (yield* engine.startRun(loaded)) as { runId: string; outputs: never[] };
  });

const flakyFlow = (flakyId: string, failures: number, retry?: unknown) => ({
  flowVersion: "1" as const,
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { payload: 1 } },
    {
      id: flakyId,
      type: "flaky",
      position: { x: 0, y: 0 },
      config: { failures },
      ...(retry ? { retry } : {}),
    },
    { id: "dl", type: "debug", position: { x: 0, y: 0 }, config: {} },
  ],
  wires: [
    { source: "n1", target: flakyId },
    { source: flakyId, target: "dl", port: DEAD_LETTER_PORT },
  ],
});

test("flaky node retried until policy bound exhausted, then stops", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter);
  await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], flakyFlow("n2", 99, { maxAttempts: 3, backoff: { initialMs: 0 } })),
      retryEngineLayer([flaky]),
    ),
  );
  expect(counter.count).toBe(3); // maxAttempts reached, then stops
});

test("flaky node recovers on retry success", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter, 2);
  const record = await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { payload: 1 } },
          {
            id: "n2",
            type: "flaky",
            position: { x: 0, y: 0 },
            config: { failures: 2 },
            retry: { maxAttempts: 4, backoff: { initialMs: 0 } },
          },
        ],
        wires: [{ source: "n1", target: "n2" }],
      }),
      retryEngineLayer([flaky]),
    ),
  );

  expect(counter.count).toBe(3);
  const outputs = (
    record as unknown as {
      outputs: { nodeId: string; emitted: { port: string; payload: unknown }[] }[];
    }
  ).outputs.filter((o) => o.nodeId === "n2");
  expect(outputs).toHaveLength(1);
  expect(outputs[0]!.emitted[0]!.port).toBe("0");
  expect(typeof outputs[0]!.emitted[0]!.payload).toBe("string");
  expect((outputs[0]!.emitted[0]!.payload as string).startsWith("ok-run-")).toBe(true);
});

test("retry policy matches error type", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter); // always NetworkError
  await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { payload: 1 } },
          {
            id: "n2",
            type: "flaky",
            position: { x: 0, y: 0 },
            config: { failures: 99 },
            retry: {
              errors: ["ValidationError"],
              maxAttempts: 3,
              backoff: { initialMs: 0 },
            },
          },
        ],
        wires: [{ source: "n1", target: "n2" }],
      }),
      retryEngineLayer([flaky]),
    ),
  );
  expect(counter.count).toBe(1);
});

test("retry policy retries matched error type", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter);
  await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { payload: 1 } },
          {
            id: "n2",
            type: "flaky",
            position: { x: 0, y: 0 },
            config: { failures: 99 },
            retry: {
              errors: ["NetworkError"],
              maxAttempts: 3,
              backoff: { initialMs: 0 },
            },
          },
        ],
        wires: [{ source: "n1", target: "n2" }],
      }),
      retryEngineLayer([flaky]),
    ),
  );
  expect(counter.count).toBe(3);
});

test("node without retry policy fails immediately (no engine default retry)", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter, 1);
  await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { payload: 1 } },
          { id: "n2", type: "flaky", position: { x: 0, y: 0 }, config: { failures: 1 } },
        ],
        wires: [{ source: "n1", target: "n2" }],
      }),
      retryEngineLayer([flaky]),
    ),
  );
  expect(counter.count).toBe(1);
});

test("exhausted message routed to wired dead letter destination", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter);
  const record = await Effect.runPromise(
    Effect.provide(
      attemptRun(
        [flaky],
        flakyFlow("n2", 99, {
          maxAttempts: 3,
          backoff: { initialMs: 0 },
        }),
      ),
      retryEngineLayer([flaky]),
    ),
  );

  const dlRecord = (
    record as unknown as { outputs: { nodeId: string; message: { body: unknown } }[] }
  ).outputs.filter((o) => o.nodeId === "dl");
  expect(dlRecord).toHaveLength(1);
  expect(dlRecord[0]!.message.body).toBe(1);
});

test("unwired dead letter: engine completes and no downstream message travels", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter);
  const record = await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { payload: 1 } },
          { id: "n2", type: "flaky", position: { x: 0, y: 0 }, config: { failures: 99 } },
          { id: "dl", type: "debug", position: { x: 0, y: 0 }, config: {} },
        ],
        wires: [
          { source: "n1", target: "n2" },
          { source: "n2", target: "dl" },
        ],
      }),
      retryEngineLayer([flaky]),
    ),
  );

  const dlRecord = (record as unknown as { outputs: { nodeId: string }[] }).outputs.filter(
    (o) => o.nodeId === "dl",
  );
  expect(dlRecord).toHaveLength(0);
});

test("backoffMs implements exponential shape", () => {
  const policy = { maxAttempts: 5, backoff: { initialMs: 10, multiplier: 3 } };
  expect(backoffMs(policy, 1)).toBe(10);
  expect(backoffMs(policy, 2)).toBe(30);
  expect(backoffMs(policy, 3)).toBe(90);
});
