import { expect, test } from "bun:test";
import { Cause, Crypto, Effect, Exit, Layer, Option } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { debugNode, injectNode } from "@effect-flow/nodes-basic";
import {
  FlowEngineService,
  FlowPersistence,
  layerFlowEngine,
  layerSqlFlowPersistence,
} from "../src/index.ts";
import { layerSqliteSqlClient } from "../src/sqlite.ts";
import type { NodeOutput } from "../src/adapter.ts";

const dbFile = () => `/tmp/opencode/persist-${crypto.randomUUID()}.db`;

const output = (runId: string, nodeId: string, messageId: string, body?: unknown): NodeOutput => ({
  runId,
  nodeId,
  message: { id: messageId, body },
  emitted: [{ port: "0", body }],
});

const withSql = <A>(
  file: string,
  effect: Effect.Effect<A, never, FlowPersistence | SqlClient.SqlClient>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.provide(
        effect,
        Layer.provideMerge(layerSqlFlowPersistence, layerSqliteSqlClient(file)),
      ),
    ),
  );

test("SQL persistence returns outputs in insertion order, including a missing body", async () => {
  const file = dbFile();
  const outputs = await withSql(
    file,
    Effect.gen(function* () {
      const persistence = yield* FlowPersistence;
      yield* persistence.recordOutput(output("r1", "b", "m2", 2));
      yield* persistence.recordOutput(output("r1", "a", "m1"));
      return Option.getOrThrow(yield* persistence.getRun("r1")).outputs;
    }),
  );
  expect(outputs.map((o) => o.nodeId)).toEqual(["b", "a"]);
  expect(outputs[0]!.message.body).toBe(2);
  expect(outputs[1]!.message.body).toBeUndefined();
});

test("SQL persistence dedupes on the Run/Node/Message key", async () => {
  const file = dbFile();
  const outputs = await withSql(
    file,
    Effect.gen(function* () {
      const persistence = yield* FlowPersistence;
      yield* persistence.recordOutput(output("r1", "a", "m1", 1));
      yield* persistence.recordOutput(output("r1", "a", "m1", 1));
      return Option.getOrThrow(yield* persistence.getRun("r1")).outputs;
    }),
  );
  expect(outputs.length).toBe(1);
});

test("stored outputs survive closing and reopening the database", async () => {
  const file = dbFile();
  await withSql(
    file,
    Effect.gen(function* () {
      yield* (yield* FlowPersistence).recordOutput(output("r1", "a", "m1", "kept"));
    }),
  );
  const outputs = await withSql(
    file,
    Effect.gen(function* () {
      return Option.getOrThrow(yield* (yield* FlowPersistence).getRun("r1")).outputs;
    }),
  );
  expect(outputs.map((o) => o.message.body)).toEqual(["kept"]);
});

test("a malformed stored row fails with an error naming the Run", async () => {
  const file = dbFile();
  const exit = await Effect.runPromise(
    Effect.exit(
      Effect.scoped(
        Effect.provide(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`
              INSERT INTO flow_outputs (run_id, node_id, message_id, seq, output)
              VALUES ('bad', 'x', 'm', 1, '{"not":"an output"}')
            `;
            return yield* (yield* FlowPersistence).getRun("bad");
          }),
          Layer.provideMerge(layerSqlFlowPersistence, layerSqliteSqlClient(file)),
        ),
      ),
    ),
  );
  expect(Exit.isFailure(exit)).toBe(true);
  const defect = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;
  expect((defect as Error).message).toContain("flow_outputs row for run bad is malformed");
});

test("engine writes through a host-supplied FlowPersistence layer", async () => {
  const writes: Array<string> = [];
  const hostPersistence = Layer.sync(FlowPersistence, () => {
    const outputs: Array<NodeOutput> = [];
    return {
      recordOutput: (out) =>
        Effect.sync(() => {
          writes.push(out.nodeId);
          outputs.push(out);
        }),
      getRun: (runId) =>
        Effect.sync(() => (outputs.length ? Option.some({ runId, outputs }) : Option.none())),
    };
  });
  const flow = {
    flowVersion: "1",
    nodes: [
      { id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } },
      { id: "sink", type: "debug", position: { x: 0, y: 0 }, config: {} },
    ],
    wires: [{ source: "src", target: "sink" }],
  };
  const record = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const engine = yield* FlowEngineService;
        return yield* engine.startRun(yield* engine.loadFlow(flow));
      }),
      layerFlowEngine({
        persistence: hostPersistence,
        declarations: [injectNode, debugNode],
      }),
    ),
  );
  expect(writes.sort()).toEqual(["sink", "src"]);
  expect(record.outputs.length).toBe(2);
});

test("run IDs come from the host-supplied Crypto layer", async () => {
  const fixedCrypto = Layer.succeed(
    Crypto.Crypto,
    Crypto.make({
      randomBytes: (size) => new Uint8Array(size).fill(0xab),
      digest: () => Effect.die("digest unused in this test"),
    }),
  );
  const flow = {
    flowVersion: "1",
    nodes: [{ id: "src", type: "inject", position: { x: 0, y: 0 }, config: { body: 1 } }],
    wires: [],
  };
  const record = await Effect.runPromise(
    Effect.provide(
      Effect.gen(function* () {
        const engine = yield* FlowEngineService;
        return yield* engine.startRun(yield* engine.loadFlow(flow));
      }),
      layerFlowEngine({
        persistence: Layer.sync(FlowPersistence, () => {
          const outputs: Array<NodeOutput> = [];
          return {
            recordOutput: (out) => Effect.sync(() => void outputs.push(out)),
            getRun: (runId) =>
              Effect.sync(() => (outputs.length ? Option.some({ runId, outputs }) : Option.none())),
          };
        }),
        declarations: [injectNode],
        crypto: fixedCrypto,
      }),
    ),
  );
  expect(record.runId).toBe(`run-${"ab".repeat(16)}`);
});
