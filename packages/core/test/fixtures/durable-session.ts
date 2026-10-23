import { appendFileSync } from "node:fs";
import { Effect, Schema } from "effect";
import { debugNode, delayNode, injectNode } from "@effect-flow/nodes-basic";
import {
  FlowEngineService,
  SqliteFlowPersistence,
  layerDurableWorkflowEngine,
  defineNode,
  layerFlowEngine,
} from "../../src/index.ts";

// One engine process. `crash` starts the Run and kills the process mid-delay on
// one fan-out branch; `resume` starts a fresh process that re-runs the same
// runId to completion.
const [mode, filename, runId] = process.argv.slice(2) as [string, string, string];

// Side-effect tally: one line per execution, so a replayed Node shows up twice.
const tapNode = defineNode("tap", Schema.Struct({ mult: Schema.Number }), (ctx) =>
  Effect.sync(() => {
    appendFileSync(`${filename}.log`, `${ctx.node.id}\n`);
    ctx.emit((ctx.message.body as number) * ctx.config.mult);
  }),
);

const flowJson = {
  flowVersion: "1",
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 21 } },
    { id: "n2", type: "tap", position: { x: 100, y: 0 }, config: { mult: 2 } },
    { id: "n3", type: "delay", position: { x: 100, y: 60 }, config: { duration: 1500 } },
    { id: "n4", type: "debug", position: { x: 200, y: 0 }, config: {} },
    { id: "n5", type: "tap", position: { x: 200, y: 60 }, config: { mult: 1 } },
  ],
  wires: [
    { source: "n1", target: "n2" },
    { source: "n1", target: "n3" },
    { source: "n2", target: "n4" },
    { source: "n3", target: "n5" },
  ],
};

const program = Effect.gen(function* () {
  const engine = yield* FlowEngineService;
  const loaded = yield* engine.loadFlow(flowJson);
  if (mode === "crash") {
    yield* Effect.forkChild(engine.startRun(loaded, { runId }));
    yield* Effect.sleep("400 millis");
    process.exit(0);
  }
  const record = yield* engine.startRun(loaded, { runId });
  console.log(
    JSON.stringify({ runId: record.runId, nodes: record.outputs.map((o) => o.nodeId).sort() }),
  );
});

await Effect.runPromise(
  Effect.provide(
    program,
    layerFlowEngine({
      adapter: SqliteFlowPersistence(filename),
      declarations: [injectNode, tapNode, delayNode, debugNode],
      workflowEngine: layerDurableWorkflowEngine(filename),
    }),
  ),
);
