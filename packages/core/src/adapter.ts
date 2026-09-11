import { Effect, Option, Schema } from "effect";

export interface NodeOutput {
  readonly runId: string;
  readonly nodeId: string;
  readonly message: {
    readonly id: string;
    readonly body: unknown;
  };
  readonly emitted: ReadonlyArray<{
    readonly port: string;
    readonly payload: unknown;
  }>;
}

export interface RunRecord {
  readonly runId: string;
  readonly outputs: ReadonlyArray<NodeOutput>;
}

export interface FlowPersistence {
  readonly recordOutput: (output: NodeOutput) => Effect.Effect<void>;
  readonly getRun: (runId: string) => Effect.Effect<Option.Option<RunRecord>>;
}

export const InMemoryFlowPersistence = (): FlowPersistence => {
  const runs = new Map<string, Array<NodeOutput>>();
  return {
    recordOutput: (output) =>
      Effect.sync(() => {
        const outputs = runs.get(output.runId);
        if (outputs) {
          outputs.push(output);
        } else {
          runs.set(output.runId, [output]);
        }
      }),
    getRun: (runId) =>
      Effect.sync(() => {
        const outputs = runs.get(runId);
        return outputs ? Option.some({ runId, outputs }) : Option.none();
      }),
  };
};

export const RunRecordSchema = Schema.Struct({
  runId: Schema.String,
  outputs: Schema.Array(Schema.Any),
});
