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

export const switchNode = defineNode(
  "switch",
  Schema.Struct({
    routes: Schema.Array(
      Schema.Struct({
        field: Schema.String,
        eq: Schema.Unknown,
        port: Schema.String,
      }),
    ),
    default: Schema.String,
  }),
  (ctx) => {
    const body = ctx.message.body as Record<string, unknown> | null | undefined;
    const route = ctx.config.routes.find(
      (route: { field: string; eq: unknown }) => body?.[route.field] === route.eq,
    );
    ctx.emit(ctx.message.body, route ? route.port : ctx.config.default);
    return Effect.void;
  },
);

export const mergeNode = defineNode("merge", Schema.Struct({}), (ctx) => {
  ctx.emit(ctx.message.body);
  return Effect.void;
});

export const delayNode = defineNode("delay", Schema.Struct({ duration: Schema.Number }), (ctx) =>
  Effect.map(Effect.sleep(ctx.config.duration), () => {
    ctx.emit(ctx.message.body);
  }),
);
