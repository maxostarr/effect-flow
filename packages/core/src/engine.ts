import { Context, Effect, Layer, Option, Schema } from "effect";
import * as Wf from "effect/unstable/workflow";
import * as WorkflowEngineModule from "effect/unstable/workflow/WorkflowEngine";
import * as AdapterModule from "./adapter.ts";
import type { NodeContext, NodeDeclaration } from "./declaration.ts";
import * as SchemaModule from "./schema.ts";

export interface NodeBinding {
  readonly node: SchemaModule.NodeSchema;
  readonly declaration: NodeDeclaration<any>;
  readonly config: any;
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
    return {
      node,
      declaration,
      config,
      invocations: node.invocations ?? declaration.invocations ?? "concurrent",
    };
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
    name: `${instanceId}/${message.id}`,
    success: Schema.Array(Schema.Struct({ port: Schema.String, payload: Schema.Unknown })),
    execute: Effect.gen(function* () {
      yield* binding.declaration.execute(ctx);
      return emitted.map((entry) => ({ port: entry.port, payload: entry.payload }));
    }),
  });

  const emittedRecords = yield* activity;
  yield* adapter.recordOutput({
    runId,
    nodeId: instanceId,
    message,
    emitted: emittedRecords as any,
  });

  const outgoing: Array<[string, SchemaModule.Message]> = [];
  for (const wire of loaded.wires) {
    if (wire.source === instanceId) {
      for (const record of emittedRecords) {
        outgoing.push([wire.target, { id: nextMessageId(), body: record.payload }]);
      }
    }
  }
  return outgoing;
});

const traverseMessages = Effect.fnUntraced(function* (
  loaded: LoadedFlow,
  adapter: AdapterModule.FlowPersistence,
  runId: string,
  nextMessageId: () => string,
  frontier: ReadonlyArray<[string, SchemaModule.Message]>,
): Effect.fn.Return<void, never, EngineRequires> {
  let pending = [...frontier];
  while (pending.length > 0) {
    const concurrent: Array<[string, SchemaModule.Message]> = [];
    const serialized = new Map<string, Array<[string, SchemaModule.Message]>>();
    for (const item of pending) {
      const binding = loaded.nodes.get(item[0]);
      if (binding && binding.invocations === "serialized") {
        const queue = serialized.get(item[0]);
        if (queue) queue.push(item);
        else serialized.set(item[0], [item]);
      } else {
        concurrent.push(item);
      }
    }
    const next: Array<[string, SchemaModule.Message]> = [];
    yield* Effect.forEach(
      concurrent,
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
    yield* Effect.forEach(
      serialized.values(),
      (queue) =>
        Effect.forEach(
          queue,
          ([instanceId, message]) =>
            deliverMessage(loaded, adapter, runId, nextMessageId, instanceId, message).pipe(
              Effect.tap((outgoing) =>
                Effect.sync(() => {
                  next.push(...outgoing);
                }),
              ),
            ),
          { concurrency: 1, discard: true },
        ),
      { concurrency: "unbounded", discard: true },
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
