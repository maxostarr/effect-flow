import { Context, Deferred, Effect, Layer, Option, Result, Schema } from "effect";
import * as Wf from "effect/unstable/workflow";
import * as WorkflowEngineModule from "effect/unstable/workflow/WorkflowEngine";
import * as AdapterModule from "./adapter.ts";
import type { NodeContext, NodeDeclaration } from "./declaration.ts";
import {
  InvalidNodeConfigError,
  type FlowLoadError,
  NodeInvocationFailure,
  UnroutedEmitError,
  UnknownNodeDeclarationError,
} from "./errors.ts";
import * as SchemaModule from "./schema.ts";
import * as ValidationModule from "./validation.ts";

export interface NodeBinding {
  readonly node: SchemaModule.NodeSchema;
  readonly declaration: NodeDeclaration<unknown>;
  readonly config: unknown;
  readonly invocations: "concurrent" | "serialized";
}

export interface LoadedFlow {
  readonly flowId: string;
  readonly flow: SchemaModule.FlowSchema;
  readonly nodes: ReadonlyMap<string, NodeBinding>;
  readonly wires: ReadonlyArray<SchemaModule.WireSchema>;
}

export type EmittedRecord = {
  port: string;
  body: unknown;
};

export type AttemptResult = Result.Result<ReadonlyArray<EmittedRecord>, NodeInvocationFailure>;

type AttemptEffect = Effect.Effect<
  ReadonlyArray<EmittedRecord>,
  NodeInvocationFailure,
  EngineRequires
>;

export type { FlowLoadError } from "./errors.ts";

export interface EngineOptions {
  readonly adapter: AdapterModule.FlowPersistence;
  readonly declarations: ReadonlyArray<NodeDeclaration<any>>;
  readonly resources?: Layer.Layer<any> | undefined;
  /**
   * WorkflowEngine layer backing Runs. Defaults to the in-memory engine; a host
   * wanting restart-safe Runs injects a persistent engine here (workflow
   * executions, activities, and durable clocks then survive and replay).
   */
  readonly workflowEngine?: Layer.Layer<Wf.WorkflowEngine.WorkflowEngine> | undefined;
}

export interface FlowEngine {
  readonly adapter: AdapterModule.FlowPersistence;
  readonly loadFlow: (json: unknown) => Effect.Effect<LoadedFlow, FlowLoadError>;
  readonly startRun: (flow: LoadedFlow) => Effect.Effect<AdapterModule.RunRecord>;
}

export class FlowEngineService extends Context.Service<FlowEngineService, FlowEngine>()(
  "effect-flow/FlowEngineService",
) {}

const firstIssuePath = (issue: unknown): ReadonlyArray<PropertyKey> => {
  const record = issue as { path?: unknown; issues?: unknown; issue?: unknown } | null | undefined;
  const path = Array.isArray(record?.path) ? record!.path : [];
  if (Array.isArray(record?.issues)) {
    for (const sub of record!.issues as ReadonlyArray<unknown>) {
      const inner = firstIssuePath(sub);
      if (inner.length > 0) return [...path, ...inner];
    }
  }
  if (record?.issue !== undefined) return [...path, ...firstIssuePath(record!.issue)];
  return path;
};

const RunPayload = Schema.Struct({
  runId: Schema.String,
  flowId: Schema.String,
});

const runWorkflow = Wf.Workflow.make("effect-flow/Run", {
  payload: RunPayload,
  idempotencyKey: (payload) => payload.runId,
});

type EngineRequires = Wf.WorkflowEngine.WorkflowEngine | Wf.WorkflowEngine.WorkflowInstance;

const makeNodeService =
  (resources: Context.Context<unknown>) =>
  <I, S>(key: Context.Key<I, S>): Effect.Effect<S, never, never> =>
    Effect.suspend(() => {
      const service = Context.getOption(resources, key);
      if (Option.isNone(service)) {
        return Effect.die(
          `effect-flow: service key '${key.key}' not provided by host resource layers`,
        );
      }
      return Effect.succeed(service.value);
    });

const resolveNode = (
  declarations: ReadonlyMap<string, NodeDeclaration<unknown>>,
  node: SchemaModule.NodeSchema,
): Effect.Effect<NodeBinding, FlowLoadError> =>
  Effect.gen(function* () {
    const declaration = declarations.get(node.type);
    if (!declaration) {
      return yield* Effect.fail(
        new UnknownNodeDeclarationError({ nodeId: node.id, nodeType: node.type }),
      );
    }
    const config = yield* Effect.mapError(
      Schema.decodeUnknownEffect(declaration.config)(node.config),
      (error) =>
        new InvalidNodeConfigError({
          nodeId: node.id,
          field: firstIssuePath(error).map(String).join(".") || "<config>",
          message: (error as { message?: string }).message ?? "invalid node config",
        }),
    );
    return {
      node,
      declaration,
      config,
      invocations: node.invocations ?? declaration.invocations ?? "concurrent",
    };
  });

/** Run-scoped plumbing shared by every Message walking the Flow against Nodes. */
interface RunSources {
  readonly loaded: LoadedFlow;
  readonly adapter: AdapterModule.FlowPersistence;
  readonly service: NodeContext<never>["service"];
  readonly runId: string;
  readonly nextMessageId: () => string;
}

const deliverMessage = Effect.fnUntraced(function* (
  sources: RunSources,
  instanceId: string,
  message: SchemaModule.Message,
): Effect.fn.Return<ReadonlyArray<[string, SchemaModule.Message]>, never, EngineRequires> {
  const { loaded, adapter, service, runId, nextMessageId } = sources;
  const binding = loaded.nodes.get(instanceId);
  if (!binding) return [];

  const activity = Wf.Activity.make({
    name: `${instanceId}/${message.id}/attempt`,
    success: Schema.Array(Schema.Struct({ port: Schema.String, body: Schema.Unknown })),
    error: Schema.Any,
    // The runtime hands Node body errors to us wrapped; declaring `unknown`
    // here forces every failure through Schema.Any instead of leaking `any`.
    // eslint-disable-next-line effecttsgo/any-unknown-in-error-context
    execute: Effect.gen(function* () {
      // Per-attempt identity comes from the retry combinator's CurrentAttempt:
      // each retry lands on a distinct Activity memoization key, so Node side
      // effects run once per attempt, never once per replay.
      const attempt = yield* Wf.Activity.CurrentAttempt;
      const emitted: Array<EmittedRecord> = [];
      const ctx: NodeContext<unknown> = {
        runId,
        node: binding.node,
        config: binding.config,
        message,
        service,
        sleep: (durationMs) =>
          // Durable wait lives HERE, outside the Node body's own clock:
          // DurableClock.sleep defers to the WorkflowEngine (durable clock for
          // long waits), so a durable adapter can restart-resume past the pause.
          Wf.DurableClock.sleep({
            name: `${instanceId}/${message.id}/sleep-${attempt}`,
            duration: `${durationMs} millis`,
          }) as unknown as Effect.Effect<void, never, never>,
        emit: (body, port = SchemaModule.DEFAULT_PORT) => {
          emitted.push({ port, body });
        },
      };
      // eslint-disable-next-line effecttsgo/any-unknown-in-error-context
      yield* binding.declaration.execute(ctx);
      return emitted.map((entry) => ({ port: entry.port, body: entry.body }));
    }),
  }) as unknown as AttemptEffect;

  const attemptEffect = activity.pipe(
    Effect.mapError(
      (error): NodeInvocationFailure =>
        new NodeInvocationFailure({
          nodeId: instanceId,
          messageId: message.id,
          cause: error ?? null,
        }),
    ),
  );

  const policy = binding.node.retry;

  // Retry Policy mapping onto the Workflow Activity combinator:
  // - retry condition = the policy's error matcher (`errors` tags, `*` = all);
  // - schedule = the policy's exponential backoff, taken as a DurableClock
  //   sleep under a stable Node/Message/attempt name so the pause survives a
  //   durable-engine restart instead of vanishing with the process;
  // - maxAttempts = the retry cap (first attempt + maxAttempts - 1 retries);
  // - non-matching errors stop the schedule and fail on the first attempt;
  // - no policy means no Activity.retry at all (no engine default retry).
  // Per-attempt info inside the Node body is pulled via Activity.CurrentAttempt.
  let backoffAttempt = 0;
  const outcome: AttemptResult = yield* Effect.result(
    policy
      ? attemptEffect.pipe(
          Wf.Activity.retry({
            while: (failure) =>
              failure instanceof NodeInvocationFailure && matchPolicyError(policy, failure.cause)
                ? Effect.suspend(() => {
                    backoffAttempt++;
                    return Wf.DurableClock.sleep({
                      name: `${instanceId}/${message.id}/backoff-${backoffAttempt}`,
                      duration: `${Math.round(backoffMs(policy, backoffAttempt))} millis`,
                    }).pipe(Effect.as(true));
                  })
                : Effect.succeed(false),
            times: Math.max(0, policy.maxAttempts - 1),
          }),
        )
      : attemptEffect,
  );

  const outcomeRecords = Result.isSuccess(outcome)
    ? Option.getOrUndefined(Result.getSuccess(outcome))
    : undefined;

  if (Result.isFailure(outcome)) {
    yield* Effect.logError(
      `[effect-flow] node '${instanceId}' did not handle message '${message.id}':`,
      Result.getFailure(outcome),
    );
  }
  // recordOutput is a memoized Activity under a stable Run/Node/Message key:
  // replayed workflows replay the recorded exit instead of re-recording, so
  // output persistence executes once per Message, not once per replay.
  yield* Wf.Activity.make({
    name: `${runId}/${instanceId}/${message.id}/record-output`,
    success: Schema.Void,
    error: Schema.Never,
    execute: adapter.recordOutput({
      runId,
      nodeId: instanceId,
      message,
      emitted: outcomeRecords ?? [{ port: SchemaModule.DEAD_LETTER_PORT, body: message.body }],
    }),
  }) as unknown as Effect.Effect<void, never, never>;

  const outgoing: Array<[string, SchemaModule.Message]> = [];
  if (outcomeRecords) {
    for (const record of outcomeRecords) {
      if (
        record.port !== SchemaModule.DEFAULT_PORT &&
        !loaded.wires.some(
          (wire) =>
            wire.source === instanceId &&
            wire.port === record.port &&
            record.port !== SchemaModule.DEAD_LETTER_PORT,
        )
      ) {
        return yield* Effect.die(
          new UnroutedEmitError({
            nodeId: instanceId,
            messageId: message.id,
            port: record.port,
          }),
        );
      }
      for (const wire of loaded.wires) {
        if (
          wire.source === instanceId &&
          (wire.port ?? SchemaModule.DEFAULT_PORT) === record.port
        ) {
          outgoing.push([wire.target, { id: nextMessageId(), body: record.body }]);
        }
      }
    }
  } else {
    const deadLetterWires = loaded.wires.filter(
      (wire) => wire.source === instanceId && wire.port === SchemaModule.DEAD_LETTER_PORT,
    );
    if (deadLetterWires.length === 0) {
      yield* Effect.log(
        `message '${message.id}' dropped at node '${instanceId}' (dead letter unwired)`,
      );
    } else {
      for (const wire of deadLetterWires) {
        outgoing.push([wire.target, { id: nextMessageId(), body: message.body }]);
      }
    }
  }
  return outgoing;
});

/** Maps the policy's error matcher (`errors` tags, `*` wildcard) onto a cause. */
const matchPolicyError = (policy: SchemaModule.RetryPolicySchema, error: unknown): boolean => {
  const tags = policy.errors ?? ["*"];
  const tag = (error as { _tag?: string })?._tag;
  return tags.includes("*") || (tag !== undefined && tags.includes(tag));
};

export const backoffMs = (policy: SchemaModule.RetryPolicySchema, attempt: number): number => {
  const { initialMs, multiplier = 2 } = policy.backoff;
  return initialMs * Math.pow(multiplier, attempt - 1);
};

const makeEngineService = (options: EngineOptions) =>
  Effect.gen(function* () {
    const adapter = options.adapter;
    const declarations = new Map<string, NodeDeclaration<unknown>>(
      options.declarations.map((declaration) => [declaration.type, declaration]),
    );
    const resources = (yield* Layer.build(
      options.resources ?? Layer.empty,
    )) as Context.Context<unknown>;
    const service = makeNodeService(resources);

    let flowCounter = 0;
    const loadedFlows = new Map<string, LoadedFlow>();

    const workflowEngine = yield* Wf.WorkflowEngine.WorkflowEngine;
    const walkMessage = Effect.fnUntraced(function* (
      sources: RunSources,
      gates: ReadonlyMap<string, Deferred.Deferred<void>>,
      instanceId: string,
      message: SchemaModule.Message,
    ): Effect.fn.Return<void, never, EngineRequires> {
      const binding = sources.loaded.nodes.get(instanceId);
      let outgoing: ReadonlyArray<[string, SchemaModule.Message]>;
      if (binding?.invocations === "serialized") {
        // Deferred-chain gate: each arrival queues behind the previous one,
        // so serialized Nodes never run overlapping executes and outputs
        // record in wire-arrival order.
        const prior = gates.get(instanceId);
        const mine = Deferred.makeUnsafe<void>();
        (gates as Map<string, Deferred.Deferred<void>>).set(instanceId, mine);
        if (prior) yield* Deferred.await(prior);
        outgoing = yield* deliverMessage(sources, instanceId, message);
        yield* Deferred.succeed(mine, undefined);
      } else {
        outgoing = yield* deliverMessage(sources, instanceId, message);
      }
      for (const [nextId, nextMessage] of outgoing) {
        yield* walkMessage(sources, gates, nextId, nextMessage);
      }
    });

    const runFlowMessages = Effect.fnUntraced(function* (
      sources: RunSources,
      frontier: ReadonlyArray<[string, SchemaModule.Message]>,
    ): Effect.fn.Return<void, never, EngineRequires> {
      const serialized = new Map<string, Deferred.Deferred<void>>();
      // Every Message walks independently: no level barrier, so a slow branch
      // never stalls the rest of the Run.
      yield* Effect.forEach(
        frontier,
        ([instanceId, message]) => walkMessage(sources, serialized, instanceId, message),
        { concurrency: "unbounded", discard: true },
      );
    });

    const handler = Effect.fnUntraced(function* (payload: {
      readonly runId: string;
      readonly flowId: string;
    }): Effect.fn.Return<void, never, EngineRequires> {
      const loaded = loadedFlows.get(payload.flowId);
      if (!loaded) return yield* Effect.die(`unknown flowId: ${payload.flowId}`);
      let counter = 0;
      const nextMessageId = () => `${payload.runId}#${counter++}`;
      const frontier: Array<[string, SchemaModule.Message]> = [...loaded.nodes.values()]
        .filter((binding) => SchemaModule.isEntryNode(binding.node))
        .map((binding) => [binding.node.id, { id: nextMessageId(), body: undefined as unknown }]);
      yield* runFlowMessages(
        { loaded, adapter, service, runId: payload.runId, nextMessageId },
        frontier,
      );
    });

    yield* workflowEngine.register(runWorkflow, (payload, _executionId) => handler(payload));

    const loadFlow: FlowEngine["loadFlow"] = (json) =>
      Effect.suspend(() => {
        const flowId = `flow-${flowCounter++}`;
        return SchemaModule.parseFlow(json).pipe(
          Effect.flatMap((flow) =>
            Effect.flatMap(ValidationModule.validateTopology(flow), () =>
              Effect.map(
                Effect.forEach(flow.nodes, (node) => resolveNode(declarations, node), {
                  concurrency: 1,
                }),
                (bindings) => {
                  const nodes = new Map<string, NodeBinding>();
                  for (const binding of bindings) {
                    nodes.set(binding.node.id, binding);
                  }
                  const loaded: LoadedFlow = { flowId, flow, nodes, wires: flow.wires };
                  return loaded;
                },
              ),
            ),
          ),
        );
      });

    const startRun: FlowEngine["startRun"] = (flow) =>
      Effect.gen(function* () {
        const runId = `run-${crypto.randomUUID()}`;
        loadedFlows.set(flow.flowId, flow);
        yield* runWorkflow
          .execute({ runId, flowId: flow.flowId })
          .pipe(
            Effect.scoped,
            Effect.provideService(Wf.WorkflowEngine.WorkflowEngine, workflowEngine),
          );
        const record = yield* adapter.getRun(runId);
        return Option.getOrThrow(record);
      });

    return { adapter, loadFlow, startRun } satisfies FlowEngine;
  });

export const layerFlowEngine = (options: EngineOptions): Layer.Layer<FlowEngineService> =>
  Layer.effect(
    FlowEngineService,
    Effect.provide(
      makeEngineService(options),
      options.workflowEngine ?? WorkflowEngineModule.layerMemory,
    ),
  );
