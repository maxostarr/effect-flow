import type { Context, Effect } from "effect";
import type * as Schema from "effect/Schema";
import type * as Schemas from "./schema.ts";

export interface NodeContext<Config = unknown> {
  readonly runId: string;
  readonly node: Schemas.NodeSchema;
  readonly config: Config;
  readonly message: Schemas.Message;
  readonly emit: (body: unknown, port?: string) => void;
  readonly service: <I, S>(key: Context.Key<I, S>) => Effect.Effect<S, never, never>;
}

export interface NodeDeclaration<Config = any> {
  readonly type: string;
  readonly config: Schema.ConstraintDecoder<Config>;
  readonly invocations?: "concurrent" | "serialized" | undefined;
  readonly execute: (ctx: NodeContext<Config>) => Effect.Effect<unknown, unknown, never>;
}

export const defineNode = <Config>(
  type: string,
  config: Schema.ConstraintDecoder<Config>,
  execute: (ctx: NodeContext<Config>) => Effect.Effect<unknown, unknown, never>,
): NodeDeclaration<Config> => ({ type, config, execute });
