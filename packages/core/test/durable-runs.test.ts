import { expect, test } from "bun:test";
import { Effect, Layer, Option, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { debugNode, delayNode, injectNode } from "@effect-flow/nodes-basic";
import {
  FlowEngineService,
  FlowPersistence,
  defineNode,
  layerDurableWorkflowEngine,
  layerFlowEngine,
  layerSqlFlowPersistence,
  type FlowLoadError,
  type RunRecord,
  type RunStatus,
} from "../src/index.ts";
import { layerSqliteSqlClient } from "../src/sqlite.ts";

const dbFile = () => `/tmp/opencode/durable-runs-${crypto.randomUUID()}.db`;

// Records every execution, so a replayed Node that ran again shows up twice.
const makeNodes = (executions: Array<string>) => {
  const tap = defineNode("tap", Schema.Struct({ mult: Schema.Number }), (ctx) =>
    Effect.sync(() => {
      executions.push(ctx.node.id);
      ctx.emit((ctx.message.body as number) * ctx.config.mult);
    }),
  );
  // Fails on its first ever execution, then succeeds; retries run after a durable backoff.
  const flaky = defineNode("flaky", Schema.Struct({}), (ctx) =>
    Effect.suspend(() => {
      executions.push(ctx.node.id);
      if (executions.filter((id) => id === ctx.node.id).length === 1) {
        return Effect.fail(new Error("transient"));
      }
      ctx.emit(ctx.message.body);
      return Effect.void;
    }),
  );
  return [injectNode, delayNode, debugNode, tap, flaky];
};

const flow = (duration: number) => ({
  flowVersion: "1",
  nodes: [
    { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: 21 } },
    { id: "wait", type: "delay", position: { x: 0, y: 0 }, config: { duration } },
    { id: "double", type: "tap", position: { x: 0, y: 0 }, config: { mult: 2 } },
    { id: "sink", type: "debug", position: { x: 0, y: 0 }, config: {} },
  ],
  wires: [
    { source: "src", target: "wait" },
    { source: "wait", target: "double" },
    { source: "double", target: "sink" },
  ],
});

const engineLayer = (file: string, executions: Array<string>) => {
  const sql = layerSqliteSqlClient(file);
  return layerFlowEngine({
    persistence: layerSqlFlowPersistence.pipe(Layer.provide(sql)),
    declarations: makeNodes(executions),
    workflowEngine: layerDurableWorkflowEngine({ sql }),
  });
};

// One engine lifetime: leaving the scope is a graceful shutdown.
const session = <A>(
  file: string,
  executions: Array<string>,
  program: Effect.Effect<A, FlowLoadError, FlowEngineService>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(Effect.provide(Effect.orDie(program), engineLayer(file, executions))),
  );

const readRegistry = <A>(
  file: string,
  program: Effect.Effect<A, never, FlowPersistence>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.provide(
        program,
        Layer.provideMerge(layerSqlFlowPersistence, layerSqliteSqlClient(file)),
      ),
    ),
  );

const statusOf = (file: string, runId: string): Promise<RunStatus | undefined> =>
  readRegistry(
    file,
    Effect.gen(function* () {
      const persistence = yield* FlowPersistence;
      for (const status of ["running", "completed", "cancelled"] as const) {
        const runs = yield* persistence.listRuns(status);
        if (runs.some((run) => run.runId === runId)) return status;
      }
      return undefined;
    }),
  );

const recordOf = (file: string, runId: string): Promise<RunRecord | undefined> =>
  readRegistry(
    file,
    Effect.map(
      Effect.flatMap(FlowPersistence, (persistence) => persistence.getRun(runId)),
      Option.getOrUndefined,
    ),
  );

const flowRowCount = (file: string): Promise<number> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.provide(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const rows = yield* sql<{ n: number }>`SELECT COUNT(*) AS n FROM flows`;
          return rows[0]!.n;
        }),
        layerSqliteSqlClient(file),
      ),
    ),
  );

test("a graceful shutdown suspends in-flight Runs and a fresh process resumes them", async () => {
  const file = dbFile();
  const executions: Array<string> = [];
  const runIds = ["run-a", "run-b", "run-c"];

  await session(
    file,
    executions,
    Effect.gen(function* () {
      const engine = yield* FlowEngineService;
      const loaded = yield* engine.loadFlow(flow(200));
      for (const runId of runIds) {
        yield* Effect.forkChild(engine.startRun(loaded, { runId }));
      }
      yield* Effect.sleep("100 millis");
    }),
  );

  for (const runId of runIds) {
    expect(await statusOf(file, runId)).toBe("running");
  }

  const recovered = await session(
    file,
    executions,
    Effect.gen(function* () {
      return yield* (yield* FlowEngineService).recoverRuns();
    }),
  );

  expect(recovered.map((record) => record.runId).sort()).toEqual(runIds);
  for (const record of recovered) {
    expect(record.outputs.map((o) => o.nodeId).sort()).toEqual(["double", "sink", "src", "wait"]);
    expect(record.outputs.find((o) => o.nodeId === "sink")?.message.body).toBe(42);
    expect(await statusOf(file, record.runId)).toBe("completed");
  }
});

test("a Run is registered as running and marked completed when it finishes", async () => {
  const file = dbFile();
  const executions: Array<string> = [];
  const record = await session(
    file,
    executions,
    Effect.gen(function* () {
      const engine = yield* FlowEngineService;
      return yield* engine.startRun(yield* engine.loadFlow(flow(10)), { runId: "run-done" });
    }),
  );

  expect(record.runId).toBe("run-done");
  expect(await statusOf(file, "run-done")).toBe("completed");
});

test("recoverRuns does not re-execute a completed Run", async () => {
  const file = dbFile();
  const executions: Array<string> = [];
  await session(
    file,
    executions,
    Effect.gen(function* () {
      const engine = yield* FlowEngineService;
      yield* engine.startRun(yield* engine.loadFlow(flow(10)), { runId: "run-once" });
    }),
  );
  const executedBefore = executions.length;

  const recovered = await session(
    file,
    executions,
    Effect.gen(function* () {
      return yield* (yield* FlowEngineService).recoverRuns();
    }),
  );

  expect(recovered).toEqual([]);
  expect(executions.length).toBe(executedBefore);
});

test("cancel makes a Run terminal: it stops, and recoverRuns never resumes it", async () => {
  const file = dbFile();
  const executions: Array<string> = [];
  await session(
    file,
    executions,
    Effect.gen(function* () {
      const engine = yield* FlowEngineService;
      const loaded = yield* engine.loadFlow(flow(300));
      yield* Effect.forkChild(engine.startRun(loaded, { runId: "run-cancel" }));
      yield* Effect.sleep("100 millis");
      yield* engine.cancel("run-cancel");
      yield* Effect.sleep("400 millis");
    }),
  );

  expect(await statusOf(file, "run-cancel")).toBe("cancelled");
  const record = await recordOf(file, "run-cancel");
  expect(record?.outputs.map((o) => o.nodeId)).toEqual(["src"]);

  const recovered = await session(
    file,
    executions,
    Effect.gen(function* () {
      return yield* (yield* FlowEngineService).recoverRuns();
    }),
  );
  expect(recovered).toEqual([]);
  expect(executions).not.toContain("double");
});

test("a Run interrupted during a retry backoff resumes and completes after restart", async () => {
  const file = dbFile();
  const executions: Array<string> = [];
  const retryFlow = {
    flowVersion: "1",
    nodes: [
      { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: 7 } },
      {
        id: "retry",
        type: "flaky",
        position: { x: 0, y: 0 },
        config: {},
        retry: { maxAttempts: 3, backoff: { initialMs: 200 } },
      },
      { id: "sink", type: "debug", position: { x: 0, y: 0 }, config: {} },
    ],
    wires: [
      { source: "src", target: "retry" },
      { source: "retry", target: "sink" },
    ],
  };

  await session(
    file,
    executions,
    Effect.gen(function* () {
      const engine = yield* FlowEngineService;
      yield* Effect.forkChild(
        engine.startRun(yield* engine.loadFlow(retryFlow), { runId: "run-retry" }),
      );
      yield* Effect.sleep("100 millis");
    }),
  );

  const recovered = await session(
    file,
    executions,
    Effect.gen(function* () {
      return yield* (yield* FlowEngineService).recoverRuns();
    }),
  );

  expect(recovered.map((record) => record.runId)).toEqual(["run-retry"]);
  expect(recovered[0]!.outputs.find((o) => o.nodeId === "sink")?.message.body).toBe(7);
  expect(executions.filter((id) => id === "retry").length).toBe(2);
});

test("starting many Runs of one Flow stores the Flow once; a changed Flow is stored separately", async () => {
  const file = dbFile();
  const executions: Array<string> = [];
  await session(
    file,
    executions,
    Effect.gen(function* () {
      const engine = yield* FlowEngineService;
      const loaded = yield* engine.loadFlow(flow(10));
      for (const runId of ["same-1", "same-2", "same-3"]) {
        yield* engine.startRun(loaded, { runId });
      }
    }),
  );
  expect(await flowRowCount(file)).toBe(1);

  await session(
    file,
    executions,
    Effect.gen(function* () {
      const engine = yield* FlowEngineService;
      yield* engine.startRun(yield* engine.loadFlow(flow(20)), { runId: "changed" });
    }),
  );
  expect(await flowRowCount(file)).toBe(2);
  expect(await statusOf(file, "same-1")).toBe("completed");
  expect(await statusOf(file, "changed")).toBe("completed");
});
