import { Effect, Layer, Option, Schema } from "effect";
import * as SqlClient from "effect/sql/SqlClient";
import { FlowPersistence, type NodeOutput, type RunRecord } from "./adapter.ts";

// `body` is optional because JSON drops `undefined`, and a Message may have no body.
const StoredOutput = Schema.Struct({
  runId: Schema.String,
  nodeId: Schema.String,
  message: Schema.Struct({ id: Schema.String, body: Schema.optionalKey(Schema.Unknown) }),
  emitted: Schema.Array(
    Schema.Struct({ port: Schema.String, body: Schema.optionalKey(Schema.Unknown) }),
  ),
});

const StoredOutputJson = Schema.fromJsonString(StoredOutput);

const encodeOutput = Schema.encodeSync(StoredOutputJson);
const decodeStored = Schema.decodeUnknownSync(StoredOutputJson);

const decodeOutput = (json: string): NodeOutput => {
  const stored = decodeStored(json);
  return {
    runId: stored.runId,
    nodeId: stored.nodeId,
    message: { id: stored.message.id, body: stored.message.body },
    emitted: stored.emitted.map((entry) => ({ port: entry.port, body: entry.body })),
  };
};

/**
 * Run outputs persisted through any `SqlClient` (SQLite, Postgres, ...). The
 * host supplies the driver, so this layer carries no driver dependency.
 *
 * Insertion order is kept in a `seq` column so reads do not depend on
 * dialect-specific row ids. `seq` is assigned in this process; running two
 * processes against one table is not supported yet.
 */
export const layerSqlFlowPersistence: Layer.Layer<FlowPersistence, never, SqlClient.SqlClient> =
  Layer.effect(
    FlowPersistence,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        CREATE TABLE IF NOT EXISTS flow_outputs (
          run_id TEXT NOT NULL,
          node_id TEXT NOT NULL,
          message_id TEXT NOT NULL,
          seq INTEGER NOT NULL,
          output TEXT NOT NULL,
          PRIMARY KEY (run_id, node_id, message_id)
        )
      `.pipe(Effect.orDie);
      const maxRows = yield* sql<{ max: number | null }>`
        SELECT MAX(seq) AS max FROM flow_outputs
      `.pipe(Effect.orDie);
      let lastSeq = maxRows[0]?.max ?? 0;

      return {
        recordOutput: (output: NodeOutput) =>
          Effect.suspend(() => {
            lastSeq += 1;
            const seq = lastSeq;
            return sql`
              INSERT INTO flow_outputs (run_id, node_id, message_id, seq, output)
              VALUES (${output.runId}, ${output.nodeId}, ${output.message.id}, ${seq}, ${encodeOutput(output)})
              ON CONFLICT (run_id, node_id, message_id) DO NOTHING
            `.pipe(Effect.asVoid, Effect.orDie);
          }),
        getRun: (runId: string) =>
          Effect.gen(function* () {
            const rows = yield* sql<{ output: string }>`
              SELECT output FROM flow_outputs WHERE run_id = ${runId} ORDER BY seq
            `.pipe(Effect.orDie);
            if (rows.length === 0) return Option.none<RunRecord>();
            const outputs = yield* Effect.forEach(rows, (row) =>
              Effect.try({
                try: () => decodeOutput(row.output),
                catch: (error) =>
                  new Error(`flow_outputs row for run ${runId} is malformed: ${String(error)}`),
              }).pipe(Effect.orDie),
            );
            return Option.some({ runId, outputs });
          }),
      };
    }),
  );
