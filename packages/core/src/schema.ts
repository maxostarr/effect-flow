import { Effect, Schema } from "effect";
import { InvalidFlowError } from "./errors.ts";

export const Message = Schema.Struct({
  id: Schema.String,
  body: Schema.Unknown,
});

export type Message = typeof Message.Type;

export const ExponentialBackoff = Schema.Struct({
  initialMs: Schema.Number,
  multiplier: Schema.optionalKey(Schema.Number),
});

export type ExponentialBackoffSchema = typeof ExponentialBackoff.Type;

export const RetryPolicy = Schema.Struct({
  errors: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  maxAttempts: Schema.Number,
  backoff: ExponentialBackoff,
});

export type RetryPolicySchema = typeof RetryPolicy.Type;

/** Port carried by a Wire routing toward a Dead Letter destination. */
export const DEAD_LETTER_PORT = "dead-letter";

export const Invocations = Schema.Literals(["concurrent", "serialized"]);

export const Node = Schema.Struct({
  id: Schema.NonEmptyString,
  type: Schema.NonEmptyString,
  position: Schema.Struct({
    x: Schema.Number,
    y: Schema.Number,
  }),
  config: Schema.Unknown,
  retry: Schema.optionalKey(RetryPolicy),
  invocations: Schema.optionalKey(Invocations),
});

export type NodeSchema = typeof Node.Type;

export const Wire = Schema.Struct({
  source: Schema.String,
  target: Schema.String,
  port: Schema.optionalKey(Schema.String),
});

export type WireSchema = typeof Wire.Type;

export const Metadata = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  description: Schema.optionalKey(Schema.String),
});

export type MetadataSchema = typeof Metadata.Type;

export const Flow = Schema.Struct({
  flowVersion: Schema.Literal("1"),
  metadata: Schema.optionalKey(Metadata),
  nodes: Schema.Array(Node),
  wires: Schema.Array(Wire),
});

export type FlowSchema = typeof Flow.Type;

export const parseFlow = (input: unknown) =>
  Schema.decodeUnknownEffect(Flow)(input).pipe(
    Effect.mapError((error) => new InvalidFlowError({ message: error.message })),
  );

/** Default ports on wires unnamed in Flow JSON; Nodes emitting here drop output if unwired. */
export const DEFAULT_PORT = "0";

/** Node types that start message traversal; engine + topology share this predicate. */
export const isEntryNode = (node: NodeSchema): boolean => node.type === "inject";
