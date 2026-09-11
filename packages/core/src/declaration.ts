import type { Effect } from "effect";
import * as Schema from "effect/Schema";
import type * as Schemas from "./schema.ts";

export interface NodeContext<Config = unknown> {
  readonly runId: string;
  readonly node: Schemas.NodeSchema;
  readonly config: Config;
  readonly message: Schemas.Message;
  readonly emit: (body: unknown, port?: string) => void;
}

export interface NodeDeclaration<Config = any> {
  readonly type: string;
  readonly config: Schema.ConstraintDecoder<Config>;
  readonly invocations?: "concurrent" | "serialized" | undefined;
  readonly execute: (ctx: NodeContext<Config>) => Effect.Effect<unknown, never, never>;
}

export const defineNode = <Config>(
  type: string,
  config: Schema.ConstraintDecoder<Config>,
  execute: (ctx: NodeContext<Config>) => Effect.Effect<unknown, never, never>,
): NodeDeclaration<Config> => ({ type, config, execute });
