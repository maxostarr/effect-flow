import { Effect } from "effect";
import {
  debugNode,
  FlowEngineService,
  InMemoryFlowPersistence,
  injectNode,
  layerFlowEngine,
  mapNode,
} from "./src/index.ts";

const sampleFlow = {
  flowVersion: "1",
  metadata: { name: "demo", description: "inject -> map -> debug" },
  nodes: [
    { id: "n1", type: "inject", position: { x: 0, y: 0 }, config: { payload: 21 } },
    { id: "n2", type: "map", position: { x: 100, y: 0 }, config: { mult: 2 } },
    { id: "n3", type: "debug", position: { x: 200, y: 0 }, config: {} },
  ],
  wires: [
    { source: "n1", target: "n2" },
    { source: "n2", target: "n3" },
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

  const debugSteps = record.outputs.filter((output) => output.nodeId === "n3");
  console.log(
    "debug reached:",
    debugSteps.length === 1,
    "with body 42:",
    debugSteps[0]?.message.body === 42,
  );
});

Effect.runPromise(
  Effect.provide(
    program,
    layerFlowEngine({
      adapter: InMemoryFlowPersistence(),
      declarations: [injectNode, mapNode, debugNode],
    }),
  ),
).then(
  () => void 0,
  () => void 0,
);
