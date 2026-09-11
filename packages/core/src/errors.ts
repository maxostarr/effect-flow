import { Schema } from "effect";

/** Raised when Flow JSON fails schema decode; names the first schema issue. */
export class InvalidFlowError extends Schema.TaggedError<InvalidFlowError>()("InvalidFlowError", {
  message: Schema.String,
}) {}

/** Load-time check naming the duplicated node id (map construction is not last-wins). */
export class DuplicateNodeError extends Schema.TaggedError<DuplicateNodeError>()(
  "DuplicateNodeError",
  {
    nodeId: Schema.String,
    message: Schema.String,
  },
) {}

export class UnknownNodeDeclarationError extends Schema.TaggedError<UnknownNodeDeclarationError>()(
  "UnknownNodeDeclarationError",
  {
    nodeId: Schema.String,
    nodeType: Schema.String,
  },
) {}

export class InvalidNodeConfigError extends Schema.TaggedError<InvalidNodeConfigError>()(
  "InvalidNodeConfigError",
  {
    nodeId: Schema.String,
    field: Schema.String,
    message: Schema.String,
  },
) {}

export class NodeInvocationFailure extends Schema.TaggedError<NodeInvocationFailure>()(
  "NodeInvocationFailure",
  {
    nodeId: Schema.String,
    messageId: Schema.String,
    cause: Schema.Any,
  },
) {}

/** Emitted on a named port with no Wire carries it onward; routes mis-routing loudly. */
export class UnroutedEmitError extends Schema.TaggedError<UnroutedEmitError>()(
  "UnroutedEmitError",
  {
    nodeId: Schema.String,
    messageId: Schema.String,
    port: Schema.String,
  },
) {}

export class FlowCycleError extends Schema.TaggedError<FlowCycleError>()("FlowCycleError", {
  nodes: Schema.Array(Schema.String),
  wires: Schema.Array(Schema.String),
  message: Schema.String,
}) {}

export class DanglingWireError extends Schema.TaggedError<DanglingWireError>()(
  "DanglingWireError",
  {
    wire: Schema.String,
    missingNodeId: Schema.String,
    message: Schema.String,
  },
) {}

export class UnreachableNodesError extends Schema.TaggedError<UnreachableNodesError>()(
  "UnreachableNodesError",
  {
    nodes: Schema.Array(Schema.String),
    message: Schema.String,
  },
) {}

export type FlowTopologyError =
  | FlowCycleError
  | DanglingWireError
  | UnreachableNodesError
  | DuplicateNodeError;

/** Everything `loadFlow` can fail with: decode, declaration, config, topology. */
export type FlowLoadError =
  | InvalidFlowError
  | UnknownNodeDeclarationError
  | InvalidNodeConfigError
  | FlowTopologyError;
