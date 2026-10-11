import type { NodeTypeSpec, Pipeline } from "./model";

export interface RunNode { id: string; type: string; status: string; message: string; rows: number | null; columns: string[] }
export interface RunReport { pipeline: string; status: string; nodes: RunNode[] }
export interface ConversionReport {
  converted: string[];
  unsupported: { step: string; type: string; reason: string }[];
  warnings: string[];
  environment: string[];
  complete: boolean;
}

let token = "";
try {
  token = sessionStorage.getItem("bocagrande-token") ?? "";
} catch {
  /* storage can be unavailable; the editor works without it */
}
export function setToken(value: string): void {
  token = value;
  try {
    sessionStorage.setItem("bocagrande-token", value);
  } catch {
    /* ignore */
  }
}
export const getToken = (): string => token;

async function call<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const response = await fetch(path, { ...init, headers });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = (body as { detail?: unknown }).detail;
    throw new Error(typeof detail === "string" ? detail : `Request failed (${response.status})`);
  }
  return body as T;
}

const json = (method: string, body: unknown): RequestInit => ({
  method,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

export const api = {
  nodeTypes: () => call<NodeTypeSpec[]>("/api/node-types"),
  validate: (p: Pipeline) => call<{ valid: boolean; problems: string[] }>("/api/pipelines/validate", json("POST", p)),
  run: (p: Pipeline) => call<RunReport>("/api/pipelines/run", json("POST", p)),
  list: () => call<{ pipelines: string[] }>("/api/pipelines"),
  load: (name: string) => call<Pipeline>(`/api/pipelines/${encodeURIComponent(name)}`),
  save: (name: string, p: Pipeline) => call<{ saved: string; problems: string[] }>(`/api/pipelines/${encodeURIComponent(name)}`, json("PUT", p)),
  importPentaho: (file: File) =>
    call<{ pipeline: Pipeline; report: ConversionReport }>("/api/pentaho/import", { method: "POST", body: file }),
};
