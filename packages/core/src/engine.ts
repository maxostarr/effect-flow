import { Context, Effect, Layer, Option, Result, Schema } from "effect";
import * as Wf from "effect/unstable/workflow";
import * as WorkflowEngineModule from "effect/unstable/workflow/WorkflowEngine";
import * as AdapterModule from "./adapter.ts";
import type { NodeContext, NodeDeclaration } from "./declaration.ts";
import * as SchemaModule from "./schema.ts";

export interface NodeBinding {
  readonly node: SchemaModule.NodeSchema;
  readonly declaration: NodeDeclaration<any>;
  readonly config: any;
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

export type AttemptResult = Result.Result<
  Array<{ port: string; payload: unknown }>,
  NodeInvocationFailure
>;

type AttemptEffect = Effect.Effect<
  Array<{ port: string; payload: unknown }>,
  NodeInvocationFailure,
  EngineRequires
>;

export type FlowLoadError =
  | SchemaModule.InvalidFlowError
  | UnknownNodeDeclarationError
  | InvalidNodeConfigError;

export interface EngineOptions {
  readonly adapter: AdapterModule.FlowPersistence;
  readonly declarations: ReadonlyArray<NodeDeclaration<any>>;
}

export interface FlowEngine {
  readonly adapter: AdapterModule.FlowPersistence;
  readonly loadFlow: (json: unknown) => Effect.Effect<LoadedFlow, FlowLoadError>;
  readonly startRun: (flow: LoadedFlow) => Effect.Effect<AdapterModule.RunRecord>;
}

export class FlowEngineService extends Context.Service<FlowEngineService, FlowEngine>()(
  "effect-flow/FlowEngineService",
) {}

const firstIssuePath = (issue: any): ReadonlyArray<PropertyKey> => {
  const path = Array.isArray(issue?.path) ? issue.path : [];
  if (Array.isArray(issue?.issues)) {
    for (const sub of issue.issues) {
      const inner = firstIssuePath(sub);
      if (inner.length > 0) return [...path, ...inner];
    }
  }
  if (issue?.issue !== undefined) return [...path, ...firstIssuePath(issue.issue)];
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

const resolveNode = (
  declarations: ReadonlyMap<string, NodeDeclaration<any>>,
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
    return { node, declaration, config };
  });

const deliverMessage = Effect.fnUntraced(function* (
  loaded: LoadedFlow,
  adapter: AdapterModule.FlowPersistence,
  runId: string,
  nextMessageId: () => string,
  instanceId: string,
  message: SchemaModule.Message,
): Effect.fn.Return<Array<[string, SchemaModule.Message]>, never, EngineRequires> {
  const binding = loaded.nodes.get(instanceId);
  if (!binding) return [];

  const runAttempt = (attemptIndex: number): AttemptEffect => {
    const emitted: Array<{ port: string; payload: unknown }> = [];
    const ctx: NodeContext<any> = {
      runId,
      node: binding.node,
      config: binding.config,
      message,
      emit: (body, port = "0") => {
        emitted.push({ port, payload: body });
      },
    };
    const activity = Wf.Activity.make({
      name: `${instanceId}/${message.id}/attempt-${attemptIndex}`,
      success: Schema.Array(Schema.Struct({ port: Schema.String, payload: Schema.Unknown })),
      error: Schema.Any,
      // eslint-disable-next-line effecttsgo/any-unknown-in-error-context
      execute: Effect.gen(function* () {
        // eslint-disable-next-line effecttsgo/any-unknown-in-error-context
        yield* binding.declaration.execute(ctx);
        return emitted.map((entry) => ({ port: entry.port, payload: entry.payload }));
      }),
    }) as unknown as Effect.Effect<
      Array<{ port: string; payload: unknown }>,
      NodeInvocationFailure,
      EngineRequires
    >;
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
    emitted: outcomeRecords
      ? (outcomeRecords as any)
      : [{ port: SchemaModule.DEAD_LETTER_PORT, payload: message.body }],
  });

  const outgoing: Array<[string, SchemaModule.Message]> = [];
  if (outcomeRecords) {
    for (const wire of loaded.wires) {
      if (wire.source === instanceId && (wire.port ?? "0") !== SchemaModule.DEAD_LETTER_PORT) {
        for (const record of outcomeRecords) {
          if ((wire.port ?? "0") !== record.port) continue;
          outgoing.push([wire.target, { id: nextMessageId(), body: record.payload }]);
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

const traverseMessages = Effect.fnUntraced(function* (
  loaded: LoadedFlow,
  adapter: AdapterModule.FlowPersistence,
  runId: string,
  nextMessageId: () => string,
  frontier: ReadonlyArray<[string, SchemaModule.Message]>,
): Effect.fn.Return<void, never, EngineRequires> {
  let pending = [...frontier];
  while (pending.length > 0) {
    const next: Array<[string, SchemaModule.Message]> = [];
    yield* Effect.forEach(
      pending,
      ([instanceId, message]) =>
        deliverMessage(loaded, adapter, runId, nextMessageId, instanceId, message).pipe(
          Effect.tap((outgoing) =>
            Effect.sync(() => {
              next.push(...outgoing);
            }),
          ),
        ),
      { concurrency: "unbounded" },
    );
    pending = next;
  }
});

const makeEngineService = (options: EngineOptions) =>
  Effect.gen(function* () {
    const adapter = options.adapter;
    const declarations = new Map<string, NodeDeclaration<any>>(
      options.declarations.map((declaration) => [declaration.type, declaration]),
    );

    let flowCounter = 0;
    const loadedFlows = new Map<string, LoadedFlow>();

    const workflowEngine = yield* Wf.WorkflowEngine.WorkflowEngine;

    const handler = Effect.fnUntraced(function* (payload: {
      readonly runId: string;
      readonly flowId: string;
    }): Effect.fn.Return<void, never, EngineRequires> {
      const loaded = loadedFlows.get(payload.flowId);
      if (!loaded) return yield* Effect.die(`unknown flowId: ${payload.flowId}`);
      let counter = 0;
      const nextMessageId = () => `${payload.runId}#${counter++}`;
      const frontier: Array<[string, SchemaModule.Message]> = [...loaded.nodes.values()]
        .filter((binding) => binding.declaration.type === "inject")
        .map((binding) => [binding.node.id, { id: nextMessageId(), body: undefined as unknown }]);
      yield* traverseMessages(loaded, adapter, payload.runId, nextMessageId, frontier);
    });

    yield* workflowEngine.register(runWorkflow, (payload, _executionId) => handler(payload));

    const loadFlow: FlowEngine["loadFlow"] = (json) =>
      Effect.suspend(() => {
        const flowId = `flow-${flowCounter++}`;
        return SchemaModule.parseFlow(json).pipe(
          Effect.flatMap((flow) =>
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
