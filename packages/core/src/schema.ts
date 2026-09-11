import { Effect, Schema } from "effect";

export interface Message {
  readonly id: string;
  readonly body: unknown;
}

export const Message = Schema.Struct({
  id: Schema.String,
  body: Schema.Unknown,
});

export const Node = Schema.Struct({
  id: Schema.NonEmptyString,
  type: Schema.NonEmptyString,
  position: Schema.Struct({
    x: Schema.Number,
    y: Schema.Number,
  }),
  config: Schema.Unknown,
});

export interface NodeSchema {
  readonly id: string;
  readonly type: string;
  readonly position: {
    readonly x: number;
    readonly y: number;
  };
  readonly config: unknown;
}

export const Wire = Schema.Struct({
  source: Schema.String,
  target: Schema.String,
});

export interface WireSchema {
  readonly source: string;
  readonly target: string;
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
