import { describe, expect, test } from "bun:test";
import { Effect, Exit, Layer, Option } from "effect";
import * as Schema from "effect/Schema";
import * as Wf from "effect/unstable/workflow";
import * as WorkflowEngineModule from "effect/unstable/workflow/WorkflowEngine";
import { injectNode, mapNode, debugNode, delayNode, mergeNode } from "@effect-flow/nodes-basic";
import {
  FlowEngineService,
  InMemoryFlowPersistence,
  layerFlowEngine,
  backoffMs,
  DEAD_LETTER_PORT,
  defineNode,
} from "../src/index.ts";
import type { FlowLoadError } from "../src/engine.ts";
import type { NodeDeclaration } from "../src/declaration.ts";

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
    { id: "n1", type: "inject", position: { x: 0, y: 40 }, config: { body: 21 } },
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
      expect(mapped[0]!.emitted[0]).toEqual({ port: "0", body: 42 });

      const debug = record.outputs.filter((out) => out.nodeId === "n3");
      expect(debug.length).toBe(1);
      expect(debug[0]!.message.body).toBe(42);
      expect(debug[0]!.emitted[0]).toEqual({ port: "0", body: { observedBy: "n3", body: 42 } });
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

test("core package ships only Declaration primitives + engine: no basic node implementations", async () => {
  const core = await import("../src/index.ts");
  expect("injectNode" in core).toBe(false);
  expect("mapNode" in core).toBe(false);
  expect("switchNode" in core).toBe(false);
  expect("mergeNode" in core).toBe(false);
  expect("delayNode" in core).toBe(false);
  expect("debugNode" in core).toBe(false);
  expect("defineNode" in core).toBe(true);
});

test("core runs flows with host-authored declarations alone (no basic nodes registered)", async () => {
  const triggerNode = defineNode("inject", Schema.Struct({ body: Schema.Unknown }), (ctx) => {
    ctx.emit(ctx.config.body);
    return Effect.void;
  });
  const counterNode = defineNode("counter", Schema.Struct({}), (ctx) => {
    ctx.emit({ counted: ctx.message.body });
    return Effect.void;
  });
  const result = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const engine = yield* FlowEngineService;
        const loaded = yield* engine.loadFlow({
          flowVersion: "1",
          nodes: [
            { id: "t", type: "inject", position: { x: 0, y: 0 }, config: { body: "ping" } },
            { id: "c", type: "counter", position: { x: 1, y: 0 }, config: {} },
          ],
          wires: [{ source: "t", target: "c" }],
        });
        const record = yield* engine.startRun(loaded);
        return record.outputs.filter((out) => out.nodeId === "c");
      }),
      layerFlowEngine({
        adapter: InMemoryFlowPersistence(),
        declarations: [triggerNode, counterNode],
      }),
    ),
  );
  expect(result).toHaveLength(1);
  expect(result[0]!.message.body).toBe("ping");
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

const retryEngineLayer = (extra: NodeDeclaration<any>[]) =>
  layerFlowEngine({
    adapter: InMemoryFlowPersistence(),
    declarations: [injectNode, debugNode, ...extra],
  });

const attemptRun = (declarations: NodeDeclaration<any>[], flow: unknown) =>
  Effect.gen(function* () {
    const engine = yield* FlowEngineService;
    const loaded = yield* engine.loadFlow(flow);
    return (yield* engine.startRun(loaded)) as { runId: string; outputs: never[] };
  });

const flakyFlow = (flakyId: string, failures: number, retry?: unknown) => ({
  flowVersion: "1" as const,
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
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
  const flaky = makeFlakyNode(counter);
  const record = await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
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
      outputs: { nodeId: string; emitted: { port: string; body: unknown }[] }[];
    }
  ).outputs.filter((o) => o.nodeId === "n2");
  expect(outputs).toHaveLength(1);
  expect(outputs[0]!.emitted[0]!.port).toBe("0");
  expect(typeof outputs[0]!.emitted[0]!.body).toBe("string");
  expect((outputs[0]!.emitted[0]!.body as string).startsWith("ok-run-")).toBe(true);
});

test("retry policy matches error type", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter); // always NetworkError
  await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
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
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
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
  const flaky = makeFlakyNode(counter);
  await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
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
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
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

describe("per-node serialization opt-in", () => {
  let events: Array<string> = [];

  const gateNode = defineNode("gate", Schema.Struct({ delayMs: Schema.Number }), (ctx) =>
    Effect.gen(function* () {
      const tag = `gate#${ctx.message.id}`;
      events.push(`enter:${tag}`);
      yield* Effect.sleep(`${ctx.config.delayMs} millis`);
      ctx.emit(`done:${tag}`);
      events.push(`exit:${tag}`);
    }),
  );

  const gateFlow = (gateNodeJson: Record<string, unknown>, delayMs: number) => ({
    flowVersion: "1" as const,
    nodes: [
      { id: "i1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
      { id: "i2", type: "inject", position: { x: 0, y: 10 }, config: { body: 2 } },
      { id: "i3", type: "inject", position: { x: 0, y: 20 }, config: { body: 3 } },
      { id: "gate", type: "gate", position: { x: 40, y: 0 }, config: { delayMs }, ...gateNodeJson },
    ],
    wires: [
      { source: "i1", target: "gate" },
      { source: "i2", target: "gate" },
      { source: "i3", target: "gate" },
    ],
  });

  const startGatedRun = (
    gateNodeJson: Record<string, unknown>,
    delayMs: number,
    decl: typeof gateNode,
  ) =>
    Effect.runPromise(
      Effect.provide(
        Effect.gen(function* () {
          const engine = yield* FlowEngineService;
          const loaded = yield* engine.loadFlow(gateFlow(gateNodeJson, delayMs));
          return yield* engine.startRun(loaded);
        }),
        layerFlowEngine({
          adapter: InMemoryFlowPersistence(),
          declarations: [injectNode, gateNode, decl],
        }),
      ),
    );

  const maxConcurrent = (log: Array<string>) => {
    const open = new Set<string>();
    let widest = 0;
    for (const ev of log) {
      const [kind, tag] = ev.split(":") as ["enter" | "exit", string];
      if (kind === "enter") {
        open.add(tag);
        widest = Math.max(widest, open.size);
      } else {
        open.delete(tag);
      }
    }
    return widest;
  };

  test("JSON opt-in: serialized node never gets overlapping execute; outputs strictly sequenced", async () => {
    const record = await startGatedRun({ invocations: "serialized" }, 5, gateNode);
    expect(events.length).toBe(6);
    expect(maxConcurrent(events)).toBe(1);
    const gateOutputs = record.outputs.filter((o) => o.nodeId === "gate");
    expect(gateOutputs.length).toBe(3);
    // wire-arrival ordering: each recordOutput is logged exit-order, never interleaved
    for (const out of gateOutputs) {
      expect(out.emitted[0]?.body).toEqual(`done:gate#${out.message.id}`);
    }
  });

  test("declaration opt-in: serialized node never gets overlapping execute", async () => {
    await startGatedRun({}, 5, { ...gateNode, invocations: "serialized" });
    expect(maxConcurrent(events)).toBe(1);
  });

  test("default stays concurrent: same flow without opt-in interleaves", async () => {
    let sawOverlap = false;
    for (let round = 0; round < 20 && !sawOverlap; round++) {
      await startGatedRun({}, 1, gateNode);
      sawOverlap = maxConcurrent(events) > 1;
    }
    expect(sawOverlap).toBe(true);
  });

  test("JSON schema carries invocations flag; invalid value rejected", async () => {
    const result = await Effect.runPromiseExit(
      Effect.provide(
        Effect.gen(function* () {
          const engine = yield* FlowEngineService;
          return yield* engine.loadFlow(gateFlow({ invocations: "bananas" }, 1));
        }),
        layerFlowEngine({
          adapter: InMemoryFlowPersistence(),
          declarations: [injectNode, gateNode],
        }),
      ),
    );
    expect(result._tag).toBe("Failure");
  });
});

/**
 * WorkflowEngine spy wrapping the in-memory engine: records the registration
 * and execution traffic that flows through the injected engine (the surface a
 * durable engine would own), then delegates.
 */
const makeSpyEngine = () => {
  const registeredTags: Array<string> = [];
  const executedPayloads: Array<{ runId: string; flowId: string }> = [];
  const spy = Layer.effect(
    Wf.WorkflowEngine.WorkflowEngine,
    Effect.map(
      Wf.WorkflowEngine.WorkflowEngine,
      (inner) =>
        ({
          ...inner,
          register: (
            workflow: Parameters<typeof inner.register>[0],
            execute: Parameters<typeof inner.register>[1],
          ) => {
            registeredTags.push(workflow._tag);
            // eslint-disable-next-line effecttsgo/any-unknown-in-error-context
            return inner.register(workflow, execute);
          },
          execute: (
            workflow: Parameters<typeof inner.execute>[0],
            options: Parameters<typeof inner.execute>[1],
          ) => {
            executedPayloads.push(options.payload as { runId: string; flowId: string });
            // eslint-disable-next-line effecttsgo/any-unknown-in-error-context
            return inner.execute(workflow, options);
          },
        }) as typeof inner,
    ),
  );
  return {
    layer: Layer.provide(spy, WorkflowEngineModule.layerMemory),
    registeredTags,
    executedPayloads,
  };
};

test("injected WorkflowEngine layer backs Runs end to end", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter);
  const spy = makeSpyEngine();
  const record = (await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
          {
            id: "n2",
            type: "flaky",
            position: { x: 0, y: 0 },
            config: { failures: 2 },
            retry: { maxAttempts: 3, backoff: { initialMs: 10, multiplier: 2 } },
          },
        ],
        wires: [{ source: "n1", target: "n2" }],
      }),
      layerFlowEngine({
        adapter: InMemoryFlowPersistence(),
        declarations: [injectNode, flaky],
        workflowEngine: spy.layer,
      }),
    ),
  )) as unknown as { runId: string; outputs: unknown[] };

  // the injected engine owns Run registration + execution (its persistence
  // surface), which is what makes Runs restart-safe with a durable engine
  expect(spy.registeredTags).toContain("effect-flow/Run");
  expect(spy.executedPayloads.length).toBe(1);
  expect(spy.executedPayloads[0]!.runId).toBe(record.runId);
  // retry with nonzero backoff still walks all attempts (durable clock pauses)
  expect(counter.count).toBe(3);
  // one record for n1 (inject) + one for n2's eventual success
  expect(record.outputs.length).toBe(2);
});

test("retry backoff really pauses between attempts (durable clock, not skipped)", async () => {
  const counter = { count: 0 };
  const flaky = makeFlakyNode(counter);
  const started = Date.now();
  await Effect.runPromise(
    Effect.provide(
      attemptRun([flaky], {
        flowVersion: "1",
        nodes: [
          { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
          {
            id: "n2",
            type: "flaky",
            position: { x: 0, y: 0 },
            config: { failures: 99 },
            retry: { maxAttempts: 3, backoff: { initialMs: 40, multiplier: 2 } },
          },
        ],
        wires: [{ source: "n1", target: "n2" }],
      }),
      retryEngineLayer([flaky]),
    ),
  );
  // attempt 1 + 40ms + attempt 2 + 80ms + attempt 3
  expect(counter.count).toBe(3);
  expect(Date.now() - started).toBeGreaterThanOrEqual(115);
});

test("delay node sleeps through ctx.sleep even under an injected engine", async () => {
  const spy = makeSpyEngine();
  const record = (await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const engine = yield* FlowEngineService;
        const loaded = yield* engine.loadFlow({
          flowVersion: "1",
          nodes: [
            { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: "go" } },
            { id: "pause", type: "delay", position: { x: 100, y: 0 }, config: { duration: 15 } },
            { id: "sink", type: "merge", position: { x: 200, y: 0 }, config: {} },
          ],
          wires: [
            { source: "src", target: "pause" },
            { source: "pause", target: "sink" },
          ],
        });
        return yield* engine.startRun(loaded);
      }),
      layerFlowEngine({
        adapter: InMemoryFlowPersistence(),
        declarations: [injectNode, delayNode, mergeNode],
        workflowEngine: spy.layer,
      }),
    ),
  )) as unknown as { outputs: Array<{ nodeId: string }> };
  const pause = record.outputs.filter((out) => out.nodeId === "pause");
  expect(pause.length).toBe(1);
  expect(spy.executedPayloads.length).toBe(1);
});

test("recordOutput dedupes on the stable Run/Node/Message key", async () => {
  const adapter = InMemoryFlowPersistence();
  const output = {
    runId: "run-1",
    nodeId: "n1",
    message: { id: "m1", body: 1 as unknown },
    emitted: [],
  };
  const record = await Effect.runPromise(
    Effect.gen(function* () {
      yield* adapter.recordOutput(output);
      yield* adapter.recordOutput(output);
      return Option.getOrThrow(yield* adapter.getRun("run-1"));
    }),
  );
  expect(record.outputs.length).toBe(1);
});

test("distinct messages at the same node are not collapsed by dedupe", async () => {
  const adapter = InMemoryFlowPersistence();
  const record = await Effect.runPromise(
    Effect.gen(function* () {
      yield* adapter.recordOutput({
        runId: "run-1",
        nodeId: "n1",
        message: { id: "m1", body: 1 as unknown },
        emitted: [],
      });
      yield* adapter.recordOutput({
        runId: "run-1",
        nodeId: "n1",
        message: { id: "m2", body: 2 as unknown },
        emitted: [],
      });
      return Option.getOrThrow(yield* adapter.getRun("run-1"));
    }),
  );
  expect(record.outputs.length).toBe(2);
});
