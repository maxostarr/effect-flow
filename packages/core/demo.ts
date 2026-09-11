import { Effect, Schema } from "effect";
import { debugNode, injectNode, mapNode } from "@effect-flow/nodes-basic";
import {
  defineNode,
  FlowEngineService,
  InMemoryFlowPersistence,
  layerFlowEngine,
} from "./src/index.ts";
import { DEAD_LETTER_PORT } from "./src/schema.ts";

class NetworkError extends Schema.TaggedError<NetworkError>()("NetworkError", {}) {}

let invocations = 0;
const flakyMapNode = defineNode("flakyMap", Schema.Struct({ mult: Schema.Number }), (ctx) =>
  Effect.suspend(() => {
    invocations++;
    console.log(`  [${ctx.node.id}] attempt ${invocations} failed (NetworkError)`);
    return Effect.fail(new NetworkError());
  }),
);

const sampleFlow = {
  flowVersion: "1",
  metadata: {
    name: "retry-demo",
    description: "inject -> flakyMap (always fails, retry maxAttempts 5) -> Dead Letter -> debug",
  },
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 21 } },
    {
      id: "n2",
      type: "flakyMap",
      position: { x: 100, y: 0 },
      config: { mult: 2 },
      retry: { maxAttempts: 5, backoff: { initialMs: 1 } },
    },
    { id: "n3", type: "debug", position: { x: 200, y: 0 }, config: {} },
  ],
  wires: [
    { source: "n1", target: "n2" },
    { source: "n2", target: "n3" },
    { source: "n2", target: "n3", port: DEAD_LETTER_PORT },
  ],
};

const program = Effect.gen(function* () {
  const engine = yield* FlowEngineService;
  const loaded = yield* engine.loadFlow(sampleFlow);
  const record = yield* engine.startRun(loaded);

  console.log("Run:", record.runId);
  for (const output of record.outputs) {
    console.log(
      `  [${output.nodeId}] got message ${output.message.id}, body:`,
      output.message.body,
    );
  }
});

Effect.runPromise(
  Effect.provide(
    program,
    layerFlowEngine({
      adapter: InMemoryFlowPersistence(),
      declarations: [injectNode, flakyMapNode, debugNode],
    }),
  ),
).then(
  () => void 0,
  () => void 0,
);

const cyclicFlow = {
  flowVersion: "1",
  metadata: { name: "cyclic demo" },
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 21 } },
    { id: "n2", type: "map", position: { x: 100, y: 0 }, config: { mult: 2 } },
    { id: "n3", type: "debug", position: { x: 200, y: 0 }, config: {} },
  ],
  wires: [
    { source: "n1", target: "n2" },
    { source: "n2", target: "n3" },
    { source: "n3", target: "n2" },
  ],
};

const cycleDemo = Effect.gen(function* () {
  const engine = yield* FlowEngineService;
  const outcome = yield* Effect.result(engine.loadFlow(cyclicFlow as never));
  if (outcome._tag === "Success") {
    console.log("unexpected: cyclic flow loaded");
  } else {
    console.log("Flow rejected:", outcome.failure.message);
  }
});

void Effect.runPromise(
  Effect.provide(
    cycleDemo,
    layerFlowEngine({
      adapter: InMemoryFlowPersistence(),
      declarations: [injectNode, mapNode, debugNode],
    }),
  ),
);
