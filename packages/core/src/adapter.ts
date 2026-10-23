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

/**
 * Output persistence port, provided as a service so hosts can supply their own
 * implementation as a Layer.
 *
 * Contract: `recordOutput` must be idempotent per stable Run/Node/Message key
 * (`runId` + `nodeId` + `message.id`). The engine already wraps each call in a
 * memoized Workflow Activity, so a replayed workflow replays the recorded exit
 * instead of re-recording; implementations should still dedupe defensively so
 * future engines or callers cannot double-record the same Message.
 */
export interface FlowPersistenceShape {
  readonly recordOutput: (output: NodeOutput) => Effect.Effect<void>;
  readonly getRun: (runId: string) => Effect.Effect<Option.Option<RunRecord>>;
}

export class FlowPersistence extends Context.Service<FlowPersistence, FlowPersistenceShape>()(
  "effect-flow/FlowPersistence",
) {}

export const layerInMemoryPersistence: Layer.Layer<FlowPersistence> = Layer.sync(
  FlowPersistence,
  (): FlowPersistenceShape => {
    const runs = new Map<string, Array<NodeOutput>>();
    return {
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
