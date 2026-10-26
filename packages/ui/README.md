# @effect-flow/ui

Read-only visual viewer for Flow JSON.

- Serve demo: `bun run server.ts` from `packages/ui`, open `http://localhost:4173?flow=/api/flow/demo` — `/?flow=` takes a URL to Flow JSON; a base64 data value also works (`data:...`).
- `FlowViewer` component is standalone: pass a `FlowSchema` (from `@effect-flow/core`) via props.
- Editing (drag/config/save) rides on the same `toReactFlow`/`FlowNode` base later — viewer is read-only by props, not by construction.
