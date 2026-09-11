# Engine runs on Effect Workflows, every Node is an Activity

Effect Flow's execution engine is built on `effect/unstable/workflow` (currently an unstable API namespace in effect@4.x): each Node invocation within a Run executes as a Workflow Activity, giving Run-level pause/resume durability (survive server restarts while waiting on timers or external calls) without hand-rolling persistence.

Alternative rejected: rolling a custom durable blueprint engine on Effect primitives — more control, but re-implements persistence/interruption/resume that Workflows already provides; the lock-in cost of the unstable Workflow API is judged lower than building durability from scratch.

Consequences:
- The engine embeds in the host application as a library (no standalone runner service in the core).
- No default retry: failed Activity invocations stay failed unless the Flow author attaches a Retry Policy.
- User Nodes declare a single `execute(ctx)` handler; long-lived resources are provided to Nodes as Effect Layers from the host, not managed by Node-level lifecycle hooks.
