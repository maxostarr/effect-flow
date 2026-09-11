import { useEffect, useState, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { FlowViewer } from "./viewer.tsx";
import type { FlowSchema } from "../../core/src/index.ts";
import "../style.css";
import "@xyflow/react/dist/style.css";

const DEMO = "/api/flow/demo";

function App(): ReactNode {
  const [flow, setFlow] = useState<FlowSchema | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const flowParam = params.get("flow") ?? DEMO;
    if (flowParam.startsWith("data:")) {
      try {
        setFlow(JSON.parse(atob(flowParam.slice("data:".length))) as FlowSchema);
        return;
      } catch (err) {
        setError(`bad base64 flow: ${String(err)}`);
        return;
      }
    }
    fetch(flowParam)
      .then((res) => {
        if (!res.ok) throw new Error(`${flowParam}: HTTP ${res.status}`);
        return res.json();
      })
      .then((json) => setFlow(json as FlowSchema))
      .catch((err: unknown) => setError(String(err)));
  }, []);

  if (error)
    return (
      <div className="viewer-error">
        {error}
        &nbsp;<a href={`/?flow=${DEMO}`}>load demo flow</a>
      </div>
    );
  if (!flow) return <div className="viewer-loading">Loading…</div>;
  return <FlowViewer flow={flow} />;
}

const root = window.document.querySelector("#root");
if (root) createRoot(root).render(<App />);
