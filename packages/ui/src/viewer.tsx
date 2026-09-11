import { useMemo, type ReactNode } from "react";
import {
  Background,
  Handle,
  Position,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
} from "@xyflow/react";
import { DEFAULT_PORT, type FlowSchema, type NodeSchema } from "@effect-flow/core";

const FLOW_NODE = "flowNode";

export interface FlowNodeData {
  readonly node: NodeSchema;
}

export function FlowNode(props: NodeProps): ReactNode {
  const data = props.data as unknown as FlowNodeData;
  const config = JSON.stringify(data.node.config ?? {}, null, 1);
  return (
    <div className="flow-node">
      <Handle type="target" position={Position.Left} isConnectable={false} />
      <div className="flow-node-type">{data.node.type}</div>
      <pre className="flow-node-config">{config}</pre>
      <Handle type="source" position={Position.Right} isConnectable={false} />
    </div>
  );
}

export const toReactFlow = (flow: FlowSchema): { nodes: Node[]; edges: Edge[] } => ({
  nodes: flow.nodes.map((n) => ({
    id: n.id,
    type: FLOW_NODE,
    position: { x: n.position.x, y: n.position.y },
    data: { node: n } satisfies FlowNodeData,
  })),
  edges: flow.wires.map((w) => ({
    id: `${w.source}:${w.port ?? DEFAULT_PORT}->${w.target}`,
    source: w.source,
    target: w.target,
  })),
});

export interface FlowViewerProps {
  readonly flow: FlowSchema;
  readonly className?: string;
}

export function FlowViewer(props: FlowViewerProps) {
  const { nodes, edges } = useMemo(() => toReactFlow(props.flow), [props.flow]);
  return (
    <div className={props.className ?? "flow-viewer"}>
      <ReactFlow
        nodeTypes={{ [FLOW_NODE]: FlowNode }}
        nodes={nodes}
        edges={edges}
        fitView
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        edgesFocusable={false}
        deleteKeyCode={null}
        zoomOnDoubleClick={false}
        preventScrolling={false}
      >
        <Background />
      </ReactFlow>
    </div>
  );
}
