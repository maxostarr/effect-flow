import { Crypto, Effect, Layer, Option } from "effect";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Database } from "bun:sqlite";
import * as Wf from "effect/workflow";
import * as ClusterWorkflowEngine from "effect/cluster/ClusterWorkflowEngine";
import * as SingleRunner from "effect/cluster/SingleRunner";
import type { FlowPersistence, NodeOutput, RunRecord } from "./adapter.ts";

/**
 * SQLite-backed output persistence. Outputs are kept in insertion order per
 * Run and deduplicated on the Run/Node/Message key.
 */
export const SqliteFlowPersistence = (filename: string): FlowPersistence => {
  const db = new Database(filename, { create: true });
  db.run(`
    CREATE TABLE IF NOT EXISTS flow_outputs (
      run_id TEXT NOT NULL,
      node_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      output TEXT NOT NULL,
      PRIMARY KEY (run_id, node_id, message_id)
    )
  `);
  return {
    recordOutput: (output: NodeOutput) =>
      Effect.sync(() => {
        db.run(
          "INSERT OR IGNORE INTO flow_outputs (run_id, node_id, message_id, output) VALUES (?, ?, ?, ?)",
          [output.runId, output.nodeId, output.message.id, JSON.stringify(output)],
        );
      }),
    getRun: (runId) =>
      Effect.sync(() => {
        const rows = db
          .query<{ output: string }, [string]>(
            "SELECT output FROM flow_outputs WHERE run_id = ? ORDER BY rowid",
          )
          .all(runId);
        if (rows.length === 0) return Option.none();
        const outputs = rows.map((row) => JSON.parse(row.output) as NodeOutput);
        return Option.some({ runId, outputs } satisfies RunRecord);
      }),
  };
};

const CryptoLayer = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, data) =>
      Effect.promise(
        async () => new Uint8Array(await crypto.subtle.digest(algorithm, new Uint8Array(data))),
      ),
  }),
);

/**
 * WorkflowEngine whose Run state (activities, durable clocks, executions) lives
 * in a SQLite file, so a Run paused on a `delay` resumes after a process restart.
 * Pass it as `workflowEngine` to `layerFlowEngine`.
 */
export const layerDurableWorkflowEngine = (
  filename: string,
): Layer.Layer<Wf.WorkflowEngine.WorkflowEngine> =>
  ClusterWorkflowEngine.layer.pipe(
    Layer.provide(SingleRunner.layer({ runnerStorage: "sql" })),
    Layer.provide(Layer.merge(SqliteClient.layer({ filename }), CryptoLayer)),
    Layer.orDie,
  );
