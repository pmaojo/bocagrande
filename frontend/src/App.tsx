import {
  addEdge, Background, Controls, MiniMap, ReactFlow, ReactFlowProvider, useEdgesState, useNodesState,
  type Connection, type Edge,
} from "@xyflow/react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, getToken, setToken, type ConversionReport, type RunReport } from "./api";
import { Inspector } from "./Inspector";
import { emptyPipeline, fromFlow, toFlow, uniqueId, type NodeTypeSpec, type Pipeline } from "./model";
import { StepNode, type StepFlowNode } from "./StepNode";

const nodeTypes = { step: StepNode };

function Editor() {
  const [specs, setSpecs] = useState<NodeTypeSpec[]>([]);
  const [base, setBase] = useState<Pipeline>(emptyPipeline());
  const [nodes, setNodes, onNodesChange] = useNodesState<StepFlowNode>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [saved, setSaved] = useState<string[]>([]);
  const [message, setMessage] = useState("");
  const [problems, setProblems] = useState<string[]>([]);
  const [run, setRun] = useState<RunReport | null>(null);
  const [imported, setImported] = useState<ConversionReport | null>(null);
  const [needsToken, setNeedsToken] = useState(false);
  const [busy, setBusy] = useState(false);
  const upload = useRef<HTMLInputElement>(null);

  const fail = (error: unknown) => {
    const text = (error as Error).message;
    if (text.includes("(401)") || text.includes("token")) setNeedsToken(true);
    setMessage(text);
  };
  const refreshSaved = useCallback(() => api.list().then((r) => setSaved(r.pipelines)).catch(fail), []);

  useEffect(() => {
    api.nodeTypes().then(setSpecs).catch(fail);
    refreshSaved();
  }, [refreshSaved]);

  const current = useCallback((): Pipeline => fromFlow(base, nodes, edges), [base, nodes, edges]);
  const open = (pipeline: Pipeline) => {
    const flow = toFlow(pipeline, specs);
    setBase(pipeline);
    setNodes(flow.nodes as StepFlowNode[]);
    setEdges(flow.edges);
    setSelected(null);
    setProblems([]);
    setRun(null);
  };

  const onConnect = useCallback((c: Connection) => setEdges((es) => addEdge(c, es)), [setEdges]);
  const selectedNode = nodes.find((n) => n.id === selected);
  const statusById = useMemo(() => Object.fromEntries((run?.nodes ?? []).map((n) => [n.id, n.status])), [run]);
  const shownNodes = useMemo(
    () => nodes.map((n) => ({ ...n, data: { ...n.data, status: statusById[n.id] } })),
    [nodes, statusById],
  );

  const add = (spec: NodeTypeSpec) => {
    const id = uniqueId(spec.type, nodes.map((n) => n.id));
    const node: StepFlowNode = {
      id, type: "step", position: { x: 40 + (nodes.length % 4) * 230, y: 40 + Math.floor(nodes.length / 4) * 130 },
      data: { type: spec.type, config: {}, label: spec.label, inputs: spec.inputs ?? [] },
    };
    setNodes((ns) => [...ns, node]);
    setSelected(id);
  };
  const patch = (id: string, change: Partial<StepFlowNode["data"]>) =>
    setNodes((ns) => ns.map((n) => (n.id === id ? { ...n, data: { ...n.data, ...change } } : n)));
  const rename = (from: string, to: string) => {
    if (nodes.some((n) => n.id === to)) return setMessage(`The id '${to}' is already used`);
    setNodes((ns) => ns.map((n) => (n.id === from ? { ...n, id: to } : n)));
    setEdges((es) => es.map((e) => ({ ...e, source: e.source === from ? to : e.source, target: e.target === from ? to : e.target })));
    setSelected(to);
  };
  const remove = (id: string) => {
    setNodes((ns) => ns.filter((n) => n.id !== id));
    setEdges((es) => es.filter((e) => e.source !== id && e.target !== id));
    setSelected(null);
  };

  const guard = async (work: () => Promise<void>) => {
    setBusy(true);
    setMessage("");
    try {
      await work();
    } catch (error) {
      fail(error);
    } finally {
      setBusy(false);
    }
  };

  const validate = () => guard(async () => {
    const r = await api.validate(current());
    setProblems(r.problems);
    setMessage(r.valid ? "The pipeline is valid" : `${r.problems.length} problem(s)`);
  });
  const execute = () => guard(async () => {
    const pipeline = current();
    const check = await api.validate(pipeline);
    setProblems(check.problems);
    if (!check.valid) return setMessage("Fix the problems before running");
    const report = await api.run(pipeline);
    setRun(report);
    setMessage(report.status === "ok" ? "Run finished" : "Run finished with failures");
  });
  const save = () => guard(async () => {
    const pipeline = current();
    const name = pipeline.name.trim();
    const r = await api.save(name, pipeline);
    setProblems(r.problems);
    setMessage(`Saved '${r.saved}'`);
    await refreshSaved();
  });
  const importFile = (file: File) => guard(async () => {
    const r = await api.importPentaho(file);
    open(r.pipeline);
    setImported(r.report);
    setMessage(r.report.complete ? "Imported completely" : `Imported; ${r.report.unsupported.length} step(s) were not converted`);
  });

  return (
    <div className="app">
      <header>
        <h1>Bocagrande</h1>
        <input aria-label="Pipeline name" value={base.name} onChange={(e) => setBase({ ...base, name: e.target.value })} />
        <select aria-label="Open a saved pipeline" value="" onChange={(e) => e.target.value && guard(async () => open(await api.load(e.target.value)))}>
          <option value="">Open…</option>
          {saved.map((s) => <option key={s}>{s}</option>)}
        </select>
        <button onClick={() => open(emptyPipeline())}>New</button>
        <button disabled={busy} onClick={validate}>Validate</button>
        <button disabled={busy} className="primary" onClick={execute}>Run</button>
        <button disabled={busy} onClick={save}>Save</button>
        <button onClick={() => upload.current?.click()}>Import Pentaho…</button>
        <input ref={upload} type="file" accept=".ktr,.kjb" hidden onChange={(e) => {
          const file = e.target.files?.[0];
          e.target.value = "";
          if (file) importFile(file);
        }} />
        {needsToken && (
          <input aria-label="API token" type="password" placeholder="API token" defaultValue={getToken()}
            onChange={(e) => setToken(e.target.value)} onBlur={() => { setNeedsToken(false); refreshSaved(); }} />
        )}
      </header>
      <aside className="palette" aria-label="Node types">
        <h2>Add a node</h2>
        {specs.map((s) => <button key={s.type} onClick={() => add(s)} title={s.type}>{s.label}</button>)}
      </aside>
      <main>
        <ReactFlow
          nodes={shownNodes} edges={edges} nodeTypes={nodeTypes}
          onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} onConnect={onConnect}
          onNodeClick={(_, n) => setSelected(n.id)} onPaneClick={() => setSelected(null)}
          deleteKeyCode={["Backspace", "Delete"]} fitView fitViewOptions={{ maxZoom: 1 }}
        >
          <Background /><Controls /><MiniMap pannable zoomable />
        </ReactFlow>
      </main>
      <aside className="side">
        {selectedNode ? (
          <Inspector
            id={selectedNode.id} data={selectedNode.data} spec={specs.find((s) => s.type === selectedNode.data.type)}
            onChange={(p) => patch(selectedNode.id, p)} onRename={(to) => rename(selectedNode.id, to)}
            onDelete={() => remove(selectedNode.id)}
          />
        ) : (
          <section className="panel"><h2>Parameters</h2>
            <small>Values for ${"{NAME}"} references, as JSON.</small>
            <textarea aria-label="Parameters" rows={4} spellCheck={false} defaultValue={JSON.stringify(base.parameters, null, 1)}
              key={base.name + Object.keys(base.parameters).length}
              onBlur={(e) => { try { setBase({ ...base, parameters: JSON.parse(e.target.value || "{}") }); } catch { setMessage("The parameters are not valid JSON"); } }} />
          </section>
        )}
        {message && <p role="status" className="message">{message}</p>}
        {problems.length > 0 && <ul className="problems">{problems.map((p) => <li key={p}>{p}</li>)}</ul>}
        {imported && (
          <section className="panel" aria-label="Import report">
            <h2>Import report</h2>
            {imported.unsupported.map((u) => <p key={u.step} className="error"><b>{u.step}</b> ({u.type}): {u.reason}</p>)}
            {imported.warnings.map((w) => <p key={w}>{w}</p>)}
            {imported.environment.length > 0 && <p>Set before running: {imported.environment.join(", ")}</p>}
          </section>
        )}
        {run && (
          <section className="panel" aria-label="Run report">
            <h2>Run: {run.status}</h2>
            {run.nodes.map((n) => (
              <p key={n.id} className={`run-${n.status}`}><b>{n.id}</b> {n.status}{n.rows != null ? ` · ${n.rows} rows` : ""}<br /><small>{n.message}</small></p>
            ))}
          </section>
        )}
      </aside>
    </div>
  );
}

export function App() {
  return <ReactFlowProvider><Editor /></ReactFlowProvider>;
}
