import { Effect } from "effect";
import * as Schema from "effect/Schema";
import { injectNode } from "@effect-flow/nodes-basic";
import {
  FlowEngineService,
  layerInMemoryPersistence,
  defineNode,
  layerFlowEngine,
} from "@effect-flow/core";

// A count node that is slow and stateful. Declared `serialized`, the engine
// never runs two invocations of it at once, so the running count is never
// interleaved even though three Messages arrive together.

const trace: Array<string> = [];
let count = 0;

const tally = defineNode("tally", Schema.Struct({ delayMs: Schema.Finite }), (ctx) =>
  Effect.gen(function* () {
    const tag = String(ctx.message.body);
    trace.push(`enter ${tag}`);
    yield* Effect.sleep(`${ctx.config.delayMs} millis`);
    count++;
    trace.push(`exit  ${tag} -> count ${count}`);
    ctx.emit({ tag, count });
  }),
);

const demoFlow = {
  flowVersion: "1" as const,
  metadata: { name: "serialized-count-demo" },
  nodes: [
    { id: "a", type: "inject", position: { x: 0, y: 0 }, config: { body: "a" } },
    { id: "b", type: "inject", position: { x: 0, y: 40 }, config: { body: "b" } },
    { id: "c", type: "inject", position: { x: 0, y: 80 }, config: { body: "c" } },
    {
      id: "count",
      type: "tally",
      position: { x: 120, y: 40 },
      config: { delayMs: 50 },
      invocations: "serialized",
    },
  ],
  wires: [
    { source: "a", target: "count" },
    { source: "b", target: "count" },
    { source: "c", target: "count" },
  ],
};

const program = Effect.gen(function* () {
  const engine = yield* FlowEngineService;
  const loaded = yield* engine.loadFlow(demoFlow);
  const record = yield* engine.startRun(loaded);
  console.log("trace:");
  for (const line of trace) console.log(`  ${line}`);
  console.log("outputs:");
  for (const out of record.outputs.filter((o) => o.nodeId === "count")) {
    console.log(`  ${JSON.stringify(out.emitted[0]?.body)}`);
  }
});

await Effect.runPromise(
  Effect.provide(
    program,
    layerFlowEngine({
      persistence: layerInMemoryPersistence,
      declarations: [injectNode, tally],
    }),
  ),
);
