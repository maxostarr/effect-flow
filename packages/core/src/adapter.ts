import { Context, Effect, Layer, Option } from "effect";

export interface NodeOutput {
  readonly runId: string;
  readonly nodeId: string;
  readonly message: {
    readonly id: string;
    readonly body: unknown;
  };
  readonly emitted: ReadonlyArray<{
    readonly port: string;
    readonly body: unknown;
  }>;
}

export interface RunRecord {
  readonly runId: string;
  readonly outputs: ReadonlyArray<NodeOutput>;
}

export type RunStatus = "running" | "completed" | "cancelled";

export interface RunRegistration {
  readonly runId: string;
  readonly status: RunStatus;
  readonly flowHash: string;
  readonly flow: unknown;
}

/**
 * Output and Run-registry persistence port, provided as a service so hosts can
 * supply their own implementation as a Layer.
 *
 * Contract: `recordOutput` must be idempotent per stable Run/Node/Message key
 * (`runId` + `nodeId` + `message.id`). The engine already wraps each call in a
 * memoized Workflow Activity, so a replayed workflow replays the recorded exit
 * instead of re-recording; implementations should still dedupe defensively so
 * future engines or callers cannot double-record the same Message.
 *
 * `registerRun` stores each distinct Flow once, keyed by `flowHash`, and records
 * the Run as `running` unless it already exists. Re-registering keeps the
 * existing status. `transitionRun` moves a Run only while it is in `from`, and
 * reports whether it did.
 */
export interface FlowPersistenceShape {
  readonly recordOutput: (output: NodeOutput) => Effect.Effect<void>;
  readonly getRun: (runId: string) => Effect.Effect<Option.Option<RunRecord>>;
  readonly registerRun: (registration: {
    readonly runId: string;
    readonly flowHash: string;
    readonly flow: unknown;
  }) => Effect.Effect<void>;
  readonly transitionRun: (runId: string, from: RunStatus, to: RunStatus) => Effect.Effect<boolean>;
  readonly listRuns: (status: RunStatus) => Effect.Effect<ReadonlyArray<RunRegistration>>;
}

export class FlowPersistence extends Context.Service<FlowPersistence, FlowPersistenceShape>()(
  "effect-flow/FlowPersistence",
) {}

export const layerInMemoryPersistence: Layer.Layer<FlowPersistence> = Layer.sync(
  FlowPersistence,
  (): FlowPersistenceShape => {
    const runs = new Map<string, Array<NodeOutput>>();
    const flows = new Map<string, unknown>();
    const registry = new Map<string, { status: RunStatus; flowHash: string }>();
    return {
      registerRun: ({ runId, flowHash, flow }) =>
        Effect.sync(() => {
          if (!flows.has(flowHash)) flows.set(flowHash, flow);
          if (!registry.has(runId)) registry.set(runId, { status: "running", flowHash });
        }),
      transitionRun: (runId, from, to) =>
        Effect.sync(() => {
          const entry = registry.get(runId);
          if (entry?.status !== from) return false;
          entry.status = to;
          return true;
        }),
      listRuns: (status) =>
        Effect.sync(() =>
          [...registry]
            .filter(([, entry]) => entry.status === status)
            .map(([runId, entry]) => ({
              runId,
              status: entry.status,
              flowHash: entry.flowHash,
              flow: flows.get(entry.flowHash),
            })),
        ),
      recordOutput: (output) =>
        Effect.sync(() => {
          const outputs = runs.get(output.runId);
          if (!outputs) {
            runs.set(output.runId, [output]);
            return;
          }
          // Replay-safety under the stable Run/Node/Message key above.
          if (
            outputs.some(
              (recorded) =>
                recorded.nodeId === output.nodeId && recorded.message.id === output.message.id,
            )
          ) {
            return;
          }
          outputs.push(output);
        }),
      getRun: (runId) =>
        Effect.sync(() => {
          const outputs = runs.get(runId);
          return outputs ? Option.some({ runId, outputs }) : Option.none();
        }),
    };
  },
);
