import { expect, test } from "bun:test";
import { Effect } from "effect";
import { delayNode, injectNode, mapNode, mergeNode, switchNode } from "../src/index.ts";
import { FlowEngineService, InMemoryFlowPersistence, layerFlowEngine } from "@effect-flow/core";

const run = (
  effect: (engine: typeof FlowEngineService.Service) => Effect.Effect<unknown, never, never>,
) => runWithAdapter(InMemoryFlowPersistence(), effect);

const runWithAdapter = (
  adapter: ReturnType<typeof InMemoryFlowPersistence>,
  effect: (engine: typeof FlowEngineService.Service) => Effect.Effect<unknown, never, never>,
) =>
  Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const engine = yield* FlowEngineService;
        return (yield* effect(engine)) as void;
      }),
      layerFlowEngine({
        adapter,
        declarations: [injectNode, mapNode, switchNode, mergeNode, delayNode],
      }),
    ),
  );

const switchFlow = (payload: unknown) => ({
  flowVersion: "1",
  nodes: [
    { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: payload } },
    {
      id: "route",
      type: "switch",
      position: { x: 100, y: 0 },
      config: {
        routes: [{ field: "kind", eq: "hot", port: "hot" }],
        default: "cold",
      },
    },
    { id: "hotSink", type: "merge", position: { x: 200, y: -50 }, config: { mult: 1 } },
    { id: "coldSink", type: "merge", position: { x: 200, y: 50 }, config: { mult: 1 } },
  ],
  wires: [
    { source: "src", target: "route" },
    { source: "route", target: "hotSink", port: "hot" },
    { source: "route", target: "coldSink", port: "cold" },
  ],
});

test("switch routes message to exactly the wire its rule selected", () =>
  run((engine) =>
    Effect.gen(function* () {
      const loaded = yield* engine.loadFlow(switchFlow({ kind: "hot" }));
      const record = yield* engine.startRun(loaded);

      expect(record.outputs.filter((out) => out.nodeId === "hotSink").length).toBe(1);
      expect(record.outputs.filter((out) => out.nodeId === "coldSink").length).toBe(0);
    }),
  ));

test("switch uses its default rule port when no route matches", () =>
  run((engine) =>
    Effect.gen(function* () {
      const loaded = yield* engine.loadFlow(switchFlow({ kind: "cold" }));
      const record = yield* engine.startRun(loaded);

      expect(record.outputs.filter((out) => out.nodeId === "hotSink").length).toBe(0);
      const cold = record.outputs.filter((out) => out.nodeId === "coldSink");
      expect(cold.length).toBe(1);
      expect(cold[0]!.message.body).toEqual({ kind: "cold" });
    }),
  ));

test("switch routing to a port with no wire dies instead of dropping silently", async () => {
  let died = false;
  try {
    await run((engine) =>
      Effect.gen(function* () {
        const loaded = yield* engine.loadFlow({
          flowVersion: "1",
          nodes: [
            { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
            {
              id: "route",
              type: "switch",
              position: { x: 100, y: 0 },
              config: {
                routes: [{ field: "k", eq: 1, port: "missing" }],
                default: "missing",
              },
            },
            { id: "sink", type: "merge", position: { x: 200, y: 0 }, config: {} },
          ],
          wires: [
            { source: "src", target: "route" },
            // wire exists, but for a different port than the switch emits
            { source: "route", target: "sink", port: "hot" },
          ],
        });
        yield* engine.startRun(loaded);
      }),
    );
  } catch {
    died = true;
  }
  expect(died).toBe(true);
});

test("unrouted names the node, message, and offending port", async () => {
  let cause: unknown;
  try {
    await run((engine) =>
      Effect.gen(function* () {
        const loaded = yield* engine.loadFlow({
          flowVersion: "1",
          nodes: [
            { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
            {
              id: "route",
              type: "switch",
              position: { x: 100, y: 0 },
              config: {
                routes: [{ field: "k", eq: 1, port: "missing" }],
                default: "weird",
              },
            },
          ],
          wires: [{ source: "src", target: "route" }],
        });
        yield* engine.startRun(loaded);
      }),
    );
  } catch (error) {
    cause = error;
  }
  const unrouted = cause as { _tag?: string; nodeId?: string; port?: string };
  expect(unrouted._tag).toBe("UnroutedEmitError");
  expect(unrouted.nodeId).toBe("route");
  expect(unrouted.port).toBe("weird");
});

test("merge accepts messages from multiple wires and emits them downstream", () =>
  run((engine) =>
    Effect.gen(function* () {
      const flowJson = {
        flowVersion: "1",
        nodes: [
          { id: "srcA", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
          { id: "srcB", type: "inject", position: { x: 0, y: 100 }, config: { body: 2 } },
          { id: "join", type: "merge", position: { x: 100, y: 50 }, config: { mult: 1 } },
          { id: "sink", type: "merge", position: { x: 200, y: 50 }, config: { mult: 1 } },
        ],
        wires: [
          { source: "srcA", target: "join" },
          { source: "srcB", target: "join" },
          { source: "join", target: "sink" },
        ],
      };
      const loaded = yield* engine.loadFlow(flowJson);
      const record = yield* engine.startRun(loaded);

      const joined = record.outputs.filter((out) => out.nodeId === "join");
      expect(joined.length).toBe(2);
      const bodies = joined.map((out) => out.message.body as number).sort((x, y) => x - y);
      expect(bodies).toEqual([1, 2]);

      const sink = record.outputs.filter((out) => out.nodeId === "sink");
      expect(sink.length).toBe(2);
    }),
  ));

test("merge has no wire between independent injects and remains fan-safe", () =>
  run((engine) =>
    Effect.gen(function* () {
      const flowJson = {
        flowVersion: "1",
        nodes: [
          { id: "srcA", type: "inject", position: { x: 0, y: 0 }, config: { body: 7 } },
          { id: "join", type: "merge", position: { x: 100, y: 50 }, config: { mult: 1 } },
        ],
        wires: [{ source: "srcA", target: "join" }],
      };
      const loaded = yield* engine.loadFlow(flowJson);
      const record = yield* engine.startRun(loaded);

      expect(record.outputs.filter((out) => out.nodeId === "join").length).toBe(1);
    }),
  ));

test("delay pauses traversal for its configured duration", () =>
  run((engine) =>
    Effect.gen(function* () {
      const flowJson = {
        flowVersion: "1",
        nodes: [
          { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: "go" } },
          { id: "pause", type: "delay", position: { x: 100, y: 0 }, config: { duration: 20 } },
          { id: "sink", type: "merge", position: { x: 200, y: 0 }, config: { mult: 1 } },
        ],
        wires: [
          { source: "src", target: "pause" },
          { source: "pause", target: "sink" },
        ],
      };
      const loaded = yield* engine.loadFlow(flowJson);
      const startedAt = Date.now();
      const record = yield* engine.startRun(loaded);
      const elapsed = Date.now() - startedAt;

      const paused = record.outputs.filter((out) => out.nodeId === "pause").length;
      expect(paused).toBe(1);
      expect(elapsed).toBeGreaterThanOrEqual(18);
    }),
  ));

test("delay output recorded after suspension completes", () =>
  run((engine) =>
    Effect.gen(function* () {
      const flowJson = {
        flowVersion: "1",
        nodes: [
          { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: "go" } },
          { id: "pause", type: "delay", position: { x: 100, y: 0 }, config: { duration: 10 } },
          { id: "after", type: "merge", position: { x: 200, y: 0 }, config: { mult: 1 } },
        ],
        wires: [
          { source: "src", target: "pause" },
          { source: "pause", target: "after" },
        ],
      };
      const loaded = yield* engine.loadFlow(flowJson);
      const record = yield* engine.startRun(loaded);

      const after = record.outputs.filter((out) => out.nodeId === "after");
      expect(after.length).toBe(1);
      expect(after[0]!.message.body).toBe("go");
    }),
  ));

test("a delayed branch does not stall sibling branches (per-message progress)", async () => {
  const started = Date.now();
  const timeline: Array<{ key: string; body: unknown; at: number }> = [];
  const backing = InMemoryFlowPersistence();
  const stampingAdapter: typeof backing = {
    recordOutput: (output) =>
      Effect.suspend(() => {
        timeline.push({ key: output.nodeId, body: output.message.body, at: Date.now() });
        return backing.recordOutput(output);
      }),
    getRun: backing.getRun,
  };
  const flowJson = {
    flowVersion: "1",
    nodes: [
      { id: "fastSrc", type: "inject", position: { x: 0, y: 0 }, config: { body: "fast" } },
      { id: "slowSrc", type: "inject", position: { x: 0, y: 80 }, config: { body: "slow" } },
      { id: "pause", type: "delay", position: { x: 100, y: 80 }, config: { duration: 60 } },
      { id: "sink", type: "merge", position: { x: 200, y: 40 }, config: {} },
    ],
    wires: [
      { source: "fastSrc", target: "sink" },
      { source: "slowSrc", target: "pause" },
      { source: "pause", target: "sink" },
    ],
  };
  await runWithAdapter(stampingAdapter, (engine) =>
    Effect.gen(function* () {
      const loaded = yield* engine.loadFlow(flowJson);
      yield* engine.startRun(loaded);
    }),
  );
  const fastSink = timeline.find((t) => t.key === "sink" && t.body === "fast")!;
  // the fast branch reaches the sink long before the 60ms pause elapses,
  // i.e. sibling branches never wait on each other
  expect(fastSink.at - started).toBeLessThan(40);
});
