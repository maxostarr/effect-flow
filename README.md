# effect-flow

To install dependencies:

```bash
bun install
```

To run:

```bash
bun run index.ts
```

## Runs: shutdown, recovery, cancellation

A Run's state lives in the host's database when the engine uses `layerDurableWorkflowEngine` with a `layerSqlFlowPersistence`. Each Run is recorded in a registry with one of three statuses:

- `running`: started and not yet finished. A Run is left `running` when the process stops before it completes.
- `completed`: finished. `recoverRuns()` never re-executes it.
- `cancelled`: stopped by `engine.cancel(runId)`. It is terminal and never resumed.

**Graceful shutdown suspends Runs.** Closing the engine's scope (the host's main scope ending) leaves in-flight Runs `running`, so a later process resumes them. The engine does not install signal handlers. A host maps SIGINT/SIGTERM onto closing its scope, for example by running its program under a runtime whose `runMain` interrupts the main fiber on those signals.

**Recovery.** `engine.recoverRuns()` resumes every `running` Run and returns their records. Call it once at startup.

**Cancellation.** `engine.cancel(runId)` interrupts the Run if this process is executing it and marks it `cancelled`. A Run already on another process is marked `cancelled` but is not interrupted there.

This project was created using `bun init` in bun v1.3.14. [Bun](https://bun.com) is a fast all-in-one JavaScript runtime.
