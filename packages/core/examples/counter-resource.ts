import { Context, Effect, Layer } from "effect";
import * as Schema from "effect/Schema";
import { injectNode } from "@effect-flow/nodes-basic";
import {
  FlowEngineService,
  InMemoryFlowPersistence,
  defineNode,
  layerFlowEngine,
} from "@effect-flow/core";

// A third-party author registers a custom Node Declaration.
// Long-lived resources are supplied by the host as Effect Layers;
// the declaration only declares config schema + execute(ctx).

// Resource key exposed to Nodes; host provides this as a Layer.
const WordLog = Context.Service<{ record: (word: string) => number }>("WordLog");

const wordCount = defineNode(
  "wordcount",
  Schema.Struct({ prefix: Schema.optionalKey(Schema.String) }),
  (ctx) =>
    Effect.gen(function* () {
      const wordLog = yield* ctx.service(WordLog);
      const word = String(ctx.message.body);
      const count = wordLog.record(word);
      ctx.emit({ word: `${ctx.config.prefix ?? ""}${word}`, count });
    }),
);

const demoFlow = {
  flowVersion: "1" as const,
  metadata: { name: "counter-resource-demo" },
  nodes: [
    { id: "src", type: "inject", position: { x: 0, y: 50 }, config: { body: "hello" } },
    { id: "wc", type: "wordcount", position: { x: 120, y: 50 }, config: { prefix: "seen:" } },
  ],
  wires: [{ source: "src", target: "wc" }],
};

const wordLogLayer = Layer.succeed(WordLog, {
  // closure state: the Layer's ctx is built once per engine → long-lived
  record: (() => {
    let recorded = 0;
    return (word: string) => {
      void word;
      recorded++;
      return recorded;
    };
  })(),
});

const program = Effect.gen(function* () {
  const engine = yield* FlowEngineService;
  const loaded = yield* engine.loadFlow(demoFlow);
  const record = yield* engine.startRun(loaded);
  for (const out of record.outputs) {
    console.log(`  ${out.nodeId} emitted ${JSON.stringify(out.emitted)}`);
  }
});

await Effect.runPromise(
  Effect.provide(
    program,
    layerFlowEngine({
      adapter: InMemoryFlowPersistence(),
      declarations: [injectNode, wordCount],
      resources: wordLogLayer,
    }),
  ),
);
