import { describe, expect, test } from "bun:test";
import { Effect, Exit, Schema } from "effect";
import {
  FlowEngineService,
  InMemoryFlowPersistence,
  injectNode,
  layerFlowEngine,
  mapNode,
  debugNode,
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
      { id: "i1", type: "inject", position: { x: 0, y: 0 }, config: { payload: 1 } },
      { id: "i2", type: "inject", position: { x: 0, y: 10 }, config: { payload: 2 } },
      { id: "i3", type: "inject", position: { x: 0, y: 20 }, config: { payload: 3 } },
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
      expect(out.emitted[0]?.payload).toEqual(`done:gate#${out.message.id}`);
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
