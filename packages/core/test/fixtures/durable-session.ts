import { Effect } from "effect";
import { debugNode, delayNode, injectNode } from "@effect-flow/nodes-basic";
import {
  FlowEngineService,
  SqliteFlowPersistence,
  layerDurableWorkflowEngine,
  layerFlowEngine,
} from "../../src/index.ts";

// One engine process. `crash` starts the Run and kills the process mid-delay;
// `resume` starts a fresh process that re-runs the same runId to completion.
const [mode, filename, runId] = process.argv.slice(2) as [string, string, string];

const flowJson = {
  flowVersion: "1",
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { body: 21 } },
    { id: "n2", type: "delay", position: { x: 100, y: 0 }, config: { duration: 1500 } },
    { id: "n3", type: "debug", position: { x: 200, y: 0 }, config: {} },
  ],
  wires: [
    { source: "n1", target: "n2" },
    { source: "n2", target: "n3" },
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
  console.log(JSON.stringify({ runId: record.runId, nodes: record.outputs.map((o) => o.nodeId) }));
});

await Effect.runPromise(
  Effect.provide(
    program,
    layerFlowEngine({
      adapter: SqliteFlowPersistence(filename),
      declarations: [injectNode, delayNode, debugNode],
      workflowEngine: layerDurableWorkflowEngine(filename),
    }),
  ),
);
