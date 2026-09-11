import { Effect, Schema } from "effect";

export interface Message {
  readonly id: string;
  readonly body: unknown;
}

export const Message = Schema.Struct({
  id: Schema.String,
  body: Schema.Unknown,
});

export const ExponentialBackoff = Schema.Struct({
  initialMs: Schema.Number,
  multiplier: Schema.optionalKey(Schema.Number),
});

export interface ExponentialBackoffSchema {
  readonly initialMs: number;
  readonly multiplier?: number | undefined;
}

export const RetryPolicy = Schema.Struct({
  errors: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
  maxAttempts: Schema.Number,
  backoff: ExponentialBackoff,
});

export interface RetryPolicySchema {
  readonly errors?: ReadonlyArray<string> | undefined;
  readonly maxAttempts: number;
  readonly backoff: ExponentialBackoffSchema;
}

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

export interface NodeSchema {
  readonly id: string;
  readonly type: string;
  readonly position: {
    readonly x: number;
    readonly y: number;
  };
  readonly config: unknown;
  readonly retry?: RetryPolicySchema | undefined;
  readonly invocations?: "concurrent" | "serialized" | undefined;
}

export const Wire = Schema.Struct({
  source: Schema.String,
  target: Schema.String,
  port: Schema.optionalKey(Schema.String),
});

export interface WireSchema {
  readonly source: string;
  readonly target: string;
  readonly port?: string | undefined;
}

export const Metadata = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  description: Schema.optionalKey(Schema.String),
});

export const Flow = Schema.Struct({
  flowVersion: Schema.Literal("1"),
  metadata: Schema.optionalKey(Metadata),
  nodes: Schema.Array(Node),
  wires: Schema.Array(Wire),
});

export type MetadataSchema = {
  readonly name?: string | undefined;
  readonly description?: string | undefined;
};

export type FlowSchema = {
  readonly flowVersion: "1";
  readonly metadata?: MetadataSchema | undefined;
  readonly nodes: ReadonlyArray<NodeSchema>;
  readonly wires: ReadonlyArray<WireSchema>;
};

export const parseFlow = (input: unknown) =>
  Schema.decodeUnknownEffect(Flow)(input).pipe(
    Effect.mapError((error) => new InvalidFlowError({ message: error.message })),
  );

export class InvalidFlowError extends Schema.TaggedError<InvalidFlowError>()("InvalidFlowError", {
  message: Schema.String,
}) {}
