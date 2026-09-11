import { Context, Deferred, Effect, Layer, Option, Result, Schema } from "effect";
import * as Wf from "effect/unstable/workflow";
import * as WorkflowEngineModule from "effect/unstable/workflow/WorkflowEngine";
import * as AdapterModule from "./adapter.ts";
import type { NodeContext, NodeDeclaration } from "./declaration.ts";
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

export class UnknownNodeDeclarationError extends Schema.TaggedError<UnknownNodeDeclarationError>()(
  "UnknownNodeDeclarationError",
  {
    nodeId: Schema.String,
    nodeType: Schema.String,
  },
) {}

export class InvalidNodeConfigError extends Schema.TaggedError<InvalidNodeConfigError>()(
  "InvalidNodeConfigError",
  {
    nodeId: Schema.String,
    field: Schema.String,
    message: Schema.String,
  },
) {}

export class NodeInvocationFailure extends Schema.TaggedError<NodeInvocationFailure>()(
  "NodeInvocationFailure",
  {
    nodeId: Schema.String,
    messageId: Schema.String,
    cause: Schema.Any,
  },
) {}

/** Emitted on a named port with no Wire carries it onward; routes mis-routing loudly. */
export class UnroutedEmitError extends Schema.TaggedError<UnroutedEmitError>()(
  "UnroutedEmitError",
  {
    nodeId: Schema.String,
    messageId: Schema.String,
    port: Schema.String,
  },
) {}

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

export type FlowLoadError =
  | SchemaModule.InvalidFlowError
  | UnknownNodeDeclarationError
  | InvalidNodeConfigError
  | ValidationModule.FlowTopologyError;

export interface EngineOptions {
  readonly adapter: AdapterModule.FlowPersistence;
  readonly declarations: ReadonlyArray<NodeDeclaration<unknown>>;
  readonly resources?: Layer.Layer<any> | undefined;
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

  const runAttempt = (attemptIndex: number): AttemptEffect => {
    const emitted: Array<EmittedRecord> = [];
    const ctx: NodeContext<never> = {
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
          name: `${instanceId}/${message.id}/sleep-${attemptIndex}`,
          duration: `${durationMs} millis`,
        }) as unknown as Effect.Effect<void, never, never>,
      emit: (body, port = SchemaModule.DEFAULT_PORT) => {
        emitted.push({ port, body });
      },
    };
    const activity = Wf.Activity.make({
      name: `${instanceId}/${message.id}/attempt-${attemptIndex}`,
      success: Schema.Array(Schema.Struct({ port: Schema.String, body: Schema.Unknown })),
      error: Schema.Any,
      // The runtime hands Node body errors to us wrapped; declaring `unknown`
      // here forces every failure through Schema.Any instead of leaking `any`.
      // eslint-disable-next-line effecttsgo/any-unknown-in-error-context
      execute: Effect.gen(function* () {
        // eslint-disable-next-line effecttsgo/any-unknown-in-error-context
        yield* binding.declaration.execute(ctx);
        return emitted.map((entry) => ({ port: entry.port, body: entry.body }));
      }),
    }) as unknown as AttemptEffect;
    return activity.pipe(
      Effect.mapError(
        (error): NodeInvocationFailure =>
          new NodeInvocationFailure({
            nodeId: instanceId,
            messageId: message.id,
            cause: error ?? null,
          }),
      ),
    );
  };

  const policy = binding.node.retry;

  const outcome: AttemptResult = policy
    ? yield* retryLoop(runAttempt, policy)
    : yield* Effect.result(runAttempt(1));

  const outcomeRecords = Result.isSuccess(outcome)
    ? Option.getOrUndefined(Result.getSuccess(outcome))
    : undefined;

  if (Result.isFailure(outcome)) {
    yield* Effect.logError(
      `[effect-flow] node '${instanceId}' did not handle message '${message.id}':`,
      Result.getFailure(outcome),
    );
  }
  yield* adapter.recordOutput({
    runId,
    nodeId: instanceId,
    message,
    emitted: outcomeRecords ?? [{ port: SchemaModule.DEAD_LETTER_PORT, body: message.body }],
  });

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

const retryLoop = (
  runAttempt: (attemptIndex: number) => AttemptEffect,
  policy: SchemaModule.RetryPolicySchema,
): Effect.Effect<AttemptResult, never, EngineRequires> =>
  Effect.suspend(() => {
    const tags = policy.errors ?? ["*"];
    const match = (error: unknown): boolean => {
      const tag = (error as { _tag?: string })?._tag;
      return tags.includes("*") || (tag !== undefined && tags.includes(tag));
    };
    let index = 1;
    const loop: Effect.Effect<AttemptResult, never, EngineRequires> = Effect.suspend(() =>
      Effect.gen(function* () {
        const result = yield* Effect.result(runAttempt(index));
        if (Result.isSuccess(result)) return result;
        const rawFailure = Result.getFailure(result);
        const failure = Option.isOption(rawFailure)
          ? Option.getOrUndefined(rawFailure)
          : rawFailure;
        if (!failure || !(failure instanceof NodeInvocationFailure) || !match(failure.cause)) {
          return result;
        }
        index++;
        if (index > policy.maxAttempts) return result;
        yield* Effect.sleep(Math.round(backoffMs(policy, index - 1)));
        return yield* loop;
      }),
    );
    return loop;
  });

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
    Effect.provide(makeEngineService(options), WorkflowEngineModule.layerMemory),
  );
