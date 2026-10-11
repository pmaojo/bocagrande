import { useEffect, useState } from "react";
import { parseJson, type FieldSpec, type NodeData, type NodeTypeSpec } from "./model";

interface Props {
  id: string;
  data: NodeData;
  spec?: NodeTypeSpec;
  onChange: (patch: Partial<NodeData>) => void;
  onRename: (id: string) => void;
  onDelete: () => void;
}

function JsonField({ spec, value, onValue }: { spec: FieldSpec; value: unknown; onValue: (v: unknown) => void }) {
  const [text, setText] = useState(value === undefined ? "" : JSON.stringify(value, null, 1));
  const [error, setError] = useState("");
  useEffect(() => setText(value === undefined ? "" : JSON.stringify(value, null, 1)), [value]);
  return (
    <>
      <textarea
        aria-label={spec.name}
        value={text}
        rows={4}
        spellCheck={false}
        onChange={(e) => setText(e.target.value)}
        onBlur={() => {
          const parsed = parseJson(text);
          setError(parsed.error ?? "");
          if (!parsed.error) onValue(parsed.value);
        }}
      />
      {error && <span className="error">Not valid JSON: {error}</span>}
    </>
  );
}

export function Inspector({ id, data, spec, onChange, onRename, onDelete }: Props) {
  const [draftId, setDraftId] = useState(id);
  useEffect(() => setDraftId(id), [id]);
  const set = (name: string, value: unknown) => {
    const config = { ...data.config };
    if (value === undefined || value === "" || value === false) delete config[name];
    else config[name] = value;
    onChange({ config });
  };
  return (
    <section className="panel" aria-label="Node settings">
      <h2>{spec?.label ?? data.type}</h2>
      <label>
        Name
        <input value={data.label} onChange={(e) => onChange({ label: e.target.value })} />
      </label>
      <label>
        Id
        <input value={draftId} onChange={(e) => setDraftId(e.target.value)} onBlur={() => draftId && draftId !== id && onRename(draftId)} />
      </label>
      {spec?.fields.map((field) => (
        <label key={field.name}>
          {field.name}
          {field.required && <b title="required"> *</b>}
          {field.kind === "json" ? (
            <JsonField spec={field} value={data.config[field.name]} onValue={(v) => set(field.name, v)} />
          ) : field.kind === "bool" ? (
            <input type="checkbox" checked={Boolean(data.config[field.name])} onChange={(e) => set(field.name, e.target.checked)} />
          ) : field.kind === "select" ? (
            <select value={String(data.config[field.name] ?? "")} onChange={(e) => set(field.name, e.target.value)}>
              <option value="">(default)</option>
              {field.options?.filter(Boolean).map((o) => <option key={o}>{o}</option>)}
            </select>
          ) : (
            <input
              type={field.kind === "number" ? "number" : "text"}
              value={String(data.config[field.name] ?? "")}
              onChange={(e) => set(field.name, field.kind === "number" && e.target.value !== "" ? Number(e.target.value) : e.target.value)}
            />
          )}
          {field.help && <small>{field.help}</small>}
        </label>
      ))}
      {!spec && (
        <pre className="raw">{JSON.stringify(data.config, null, 1)}</pre>
      )}
      <button className="danger" onClick={onDelete}>Delete node</button>
    </section>
  );
}
