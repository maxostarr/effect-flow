import index from "./index.html";
import demoFlow from "./fixtures/demo-flow.json";

const server = Bun.serve({
  port: 4173,
  routes: {
    "/": index,
    "/api/flow/demo": () => Response.json(demoFlow),
  },
});

console.log(`Flow viewer on http://localhost:${server.port}/?flow=/api/flow/demo`);
