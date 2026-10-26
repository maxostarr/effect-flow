import { Effect } from "effect";
import { delayNode, injectNode, mapNode, mergeNode, switchNode } from "@effect-flow/nodes-basic";
import { FlowEngineService, InMemoryFlowPersistence, layerFlowEngine } from "@effect-flow/core";

const delayMs = Number(process.env.DEMO_DELAY_MS ?? 30_000);

const demoFlow = {
  flowVersion: "1" as const,
  metadata: { name: "branching-delay-demo" },
  nodes: [
    {
      id: "src",
      type: "inject",
      position: { x: 0, y: 50 },
      config: { body: { kind: "hot", value: 10 } },
    },
    {
      id: "route",
      type: "switch",
      position: { x: 120, y: 50 },
      config: {
        routes: [{ field: "kind", eq: "hot", port: "hot" }],
        default: "cold",
      },
    },
    { id: "pause", type: "delay", position: { x: 240, y: 0 }, config: { duration: delayMs } },
    { id: "cold", type: "merge", position: { x: 240, y: 120 }, config: { mult: 1 } },
    { id: "join", type: "merge", position: { x: 360, y: 50 }, config: { mult: 1 } },
    { id: "sink", type: "merge", position: { x: 480, y: 50 }, config: { mult: 1 } },
  ],
  wires: [
    { source: "src", target: "route" },
    { source: "route", target: "pause", port: "hot" },
    { source: "route", target: "cold", port: "cold" },
    { source: "pause", target: "join" },
    { source: "cold", target: "join" },
    { source: "join", target: "sink" },
  ],
};

const program = Effect.gen(function* () {
  const engine = yield* FlowEngineService;
  const loaded = yield* engine.loadFlow(demoFlow);
  console.log(`run started; pausing ${delayMs}ms mid-flight on the hot branch...`);
  const record = yield* engine.startRun(loaded);
  console.log(`run finished: ${record.outputs.length} node outputs recorded`);
  for (const out of record.outputs) {
    console.log(`  ${out.nodeId} <- ${JSON.stringify(out.message.body)}`);
  }
});

await Effect.runPromise(
  Effect.provide(
    program,
    layerFlowEngine({
      adapter: InMemoryFlowPersistence(),
      declarations: [injectNode, switchNode, delayNode, mergeNode, mapNode],
    }),
  ),
);
