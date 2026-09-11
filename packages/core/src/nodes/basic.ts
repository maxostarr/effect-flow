import { Effect } from "effect";
import * as Schema from "effect/Schema";
import { defineNode } from "../declaration.ts";

export const injectNode = defineNode(
  "inject",
  Schema.Struct({ payload: Schema.Unknown }),
  (ctx) => {
    ctx.emit(ctx.config.payload);
    return Effect.void;
  },
);

export const mapNode = defineNode("map", Schema.Struct({ mult: Schema.Number }), (ctx) => {
  ctx.emit((ctx.message.body as number) * ctx.config.mult);
  return Effect.void;
});

export const debugNode = defineNode("debug", Schema.Struct({}), (ctx) => {
  ctx.emit({ observedBy: ctx.node.id, body: ctx.message.body });
  return Effect.void;
});
