"""HTTP service around the pipeline engine, for the pipeline editor.

Everything runs inside one workspace directory: file paths are sandboxed to it, SQLite files must live in it,
environment variables are not readable from pipelines, and Pentaho files are parsed in memory (nothing is
written to disk under a name the client chose). It listens on localhost by default; set
``BOCAGRANDE_API_TOKEN`` to require ``Authorization: Bearer <token>``. A pipeline may still connect to any
database server it is given a URL for, so do not expose this service to people you would not hand a shell.
"""
from __future__ import annotations

import hmac
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles

from bocagrande.pentaho import PentahoParseError, import_file
from bocagrande.pipeline import Context, Pipeline, PipelineError, run_pipeline
from bocagrande.pipeline.catalog import catalog

MAX_UPLOAD_BYTES = 5 * 1024 * 1024
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$")
SQLITE_PREFIX = "sqlite:///"


def _confine_sqlite(pipeline: Pipeline, workspace: Path) -> None:
    """Make every SQLite URL point inside the workspace (relative paths are taken from it)."""
    for node in pipeline.nodes:
        url = node.config.get("connection_string")
        if not isinstance(url, str) or not url.lower().startswith("sqlite"):
            continue
        if not url.startswith(SQLITE_PREFIX) or "?" in url:
            raise PipelineError(f"Node '{node.id}': only plain sqlite:/// URLs are allowed")
        raw = url[len(SQLITE_PREFIX):]
        if raw in ("", ":memory:"):
            continue
        path = (workspace / raw).resolve()
        try:
            path.relative_to(workspace.resolve())
        except ValueError:
            raise PipelineError(f"Node '{node.id}': the SQLite file is outside the workspace") from None
        node.config["connection_string"] = f"{SQLITE_PREFIX}{path}"


def _parse(body: Any) -> Pipeline:
    try:
        return Pipeline.from_dict(body)
    except PipelineError as exc:
        raise HTTPException(422, str(exc)) from None


def create_app(workspace: Optional[Path] = None, frontend: Optional[Path] = None) -> FastAPI:
    workspace = Path(workspace or os.environ.get("BOCAGRANDE_WORKSPACE", ".")).resolve()
    token = os.environ.get("BOCAGRANDE_API_TOKEN", "")
    bearer = HTTPBearer(auto_error=False)

    def authorize(credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer)) -> None:
        if token and not (credentials and hmac.compare_digest(credentials.credentials, token)):
            raise HTTPException(401, "Missing or wrong token")

    app = FastAPI(title="Bocagrande pipelines", dependencies=[Depends(authorize)])
    store = workspace / "pipelines"

    def stored(name: str) -> Path:
        if not NAME.match(name):
            raise HTTPException(400, "Use letters, digits, '_', '-' and '.' (at most 100 characters) in names")
        return store / f"{name}.yaml"

    @app.exception_handler(PipelineError)
    async def pipeline_error(_: Request, exc: PipelineError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=422)

    @app.get("/api/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/node-types")
    def node_types() -> Any:
        return catalog()

    @app.post("/api/pipelines/validate")
    def validate(body: Dict[str, Any]) -> Dict[str, Any]:
        problems = _parse(body).validate()
        return {"valid": not problems, "problems": problems}

    @app.post("/api/pipelines/run")
    def run(body: Dict[str, Any]) -> Dict[str, Any]:
        pipeline = _parse(body)
        _confine_sqlite(pipeline, workspace)
        return run_pipeline(pipeline, Context(base_dir=workspace, sandbox=True, allow_env=False)).to_dict()

    @app.post("/api/pentaho/import")
    async def pentaho_import(request: Request) -> Dict[str, Any]:
        """The body is the raw .ktr or .kjb file."""
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > MAX_UPLOAD_BYTES:
            raise HTTPException(413, "The file is too large")
        data = bytearray()
        async for chunk in request.stream():
            data += chunk
            if len(data) > MAX_UPLOAD_BYTES:
                raise HTTPException(413, "The file is too large")
        try:
            pipeline, report = import_file(bytes(data))
        except PentahoParseError as exc:
            raise HTTPException(422, str(exc)) from None
        return {"pipeline": pipeline.to_dict(), "report": report.to_dict()}

    @app.get("/api/pipelines")
    def list_pipelines() -> Dict[str, Any]:
        return {"pipelines": sorted(p.stem for p in store.glob("*.yaml"))} if store.is_dir() else {"pipelines": []}

    @app.get("/api/pipelines/{name}")
    def read(name: str) -> Dict[str, Any]:
        path = stored(name)
        if not path.is_file():
            raise HTTPException(404, "No such pipeline")
        return Pipeline.from_yaml(path.read_text(encoding="utf-8")).to_dict()

    @app.put("/api/pipelines/{name}")
    def save(name: str, body: Dict[str, Any]) -> Dict[str, Any]:
        path = stored(name)
        pipeline = _parse(body)
        store.mkdir(parents=True, exist_ok=True)
        path.write_text(pipeline.to_yaml(), encoding="utf-8")
        return {"saved": name, "problems": pipeline.validate()}

    @app.delete("/api/pipelines/{name}")
    def delete(name: str) -> Dict[str, str]:
        path = stored(name)
        if not path.is_file():
            raise HTTPException(404, "No such pipeline")
        path.unlink()
        return {"deleted": name}

    if frontend and Path(frontend).is_dir():
        app.mount("/", StaticFiles(directory=str(frontend), html=True), name="editor")
    return app


def main() -> None:  # pragma: no cover - manual entrypoint
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(description="Serve the Bocagrande pipeline editor and API")
    parser.add_argument("--workspace", default=".", help="directory pipelines read and write (default: current)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--frontend", default=str(Path(__file__).resolve().parents[2] / "frontend" / "dist"))
    args = parser.parse_args()
    uvicorn.run(create_app(Path(args.workspace), Path(args.frontend)), host=args.host, port=args.port)


if __name__ == "__main__":  # pragma: no cover
    main()
