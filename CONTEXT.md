# Effect Flow

An Effect-based replacement for Node-RED: durable flow execution with a visual editor.

## Language

### Structure

**Flow**:
A collection of isolated Nodes connected by Wires that together perform computation.
_Avoid_: pipeline, graph, program

**Node**:
An isolated processing step with defined inputs and outputs, connected to other Nodes by Wires. Every Node is executed as an Effect Workflow Activity.
_Avoid_: step, block, element

**Wire**:
A directed connection from one Node's output to another Node's input through which Messages travel.
_Avoid_: edge, link, connection

**Input**:
The point on a Node where a Wire delivers Messages to it.
_Avoid_: source, inlet

**Output**:
The point on a Node from which a Wire takes Messages onward.
_Avoid_: sink, outlet, target

### Execution

**Message**:
An independently-traveling unit of data passed from one Node to the next over a Wire.
_Avoid_: event, payload, record

**Run**:
A single durable execution of a Flow: Messages enter, Nodes process, the Run survives server restarts while paused (timers, external calls).
_Avoid_: execution, invocation, instance

**Activity**:
The Effect Workflow unit wrapping one invocation of a Node's execute within a Run; the engine treats every Node as an Activity.
_Avoid_: task, job, handler

**Node Declaration**:
The user-defined definition of a Node type: configuration schema (an Effect Schema, validated by the engine on flow load), plus an execute handler, registered with the engine for use in Flows.
_Avoid_: node type, node definition, plugin

**Retry Policy**:
A user-defined rule for when and how a failed invocation of a Node is retried: an error matcher (list of error `_tag`s, absent = all errors), a maxAttempts bound, and an exponential backoff shape (initialMs, multiplier). Attached per Node in the Flow JSON (`node.retry`); the engine applies none by default. When an error matcher does not match a failure, that failure is not retried.
_Avoid_: retry config, backoff config

**Dead Letter**:
The destination a Message reaches when its delivery exhausts the retry policy of a Node. Wired as a Wire from the Node's output with port `"dead-letter"`; the Message travels over it with its original body. Flows without one simply log and drop it.
_Avoid_: error queue, catch-all
