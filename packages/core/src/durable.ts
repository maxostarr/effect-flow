import { Crypto, Layer } from "effect";
import type * as SqlClient from "effect/sql/SqlClient";
import * as Wf from "effect/workflow";
import * as ClusterWorkflowEngine from "effect/cluster/ClusterWorkflowEngine";
import * as MessageStorage from "effect/cluster/MessageStorage";
import * as Sharding from "effect/cluster/Sharding";
import * as SingleRunner from "effect/cluster/SingleRunner";
import { layerWebCrypto } from "./crypto.ts";

export interface DurableWorkflowOptions {
  /** SQL database holding workflow state. Any infallible `SqlClient` driver works. */
  readonly sql: Layer.Layer<SqlClient.SqlClient>;
  /** Randomness and digests for workflow IDs. Defaults to Web Crypto. */
  readonly crypto?: Layer.Layer<Crypto.Crypto> | undefined;
  /**
   * Sharding and message storage for the cluster that runs the workflows.
   * Defaults to a single in-process runner with SQL-backed storage; supply a
   * multi-runner setup here to scale out.
   */
  readonly cluster?:
    | Layer.Layer<
        Sharding.Sharding | MessageStorage.MessageStorage,
        never,
        SqlClient.SqlClient | Crypto.Crypto
      >
    | undefined;
}

const singleRunnerCluster = Layer.orDie(SingleRunner.layer({ runnerStorage: "sql" }));

/**
 * WorkflowEngine whose Run state (activities, durable clocks, executions) lives
 * in the host's SQL database, so a Run paused on a `delay` resumes after a
 * process restart. Pass the result as `workflowEngine` to `layerFlowEngine`.
 */
export const layerDurableWorkflowEngine = (
  options: DurableWorkflowOptions,
): Layer.Layer<Wf.WorkflowEngine.WorkflowEngine> =>
  ClusterWorkflowEngine.layer.pipe(
    Layer.provide(options.cluster ?? singleRunnerCluster),
    Layer.provide(Layer.merge(options.sql, options.crypto ?? layerWebCrypto)),
    Layer.orDie,
  );
