import pandas as pd
import pytest
from fastapi.testclient import TestClient

from adapter.db_io import write_database
from bocagrande.api import create_app
from bocagrande.pipeline.catalog import CATALOG
from bocagrande.pipeline.nodes import REGISTRY

PIPELINE = {"name": "p", "nodes": [
    {"id": "in", "type": "source.file", "config": {"path": "in.csv"}},
    {"id": "out", "type": "destination.file", "config": {"path": "out.csv"}}],
    "edges": [{"source": "in", "target": "out"}]}


@pytest.fixture
def client(tmp_path):
    pd.DataFrame({"a": [1, 2]}).to_csv(tmp_path / "in.csv", index=False)
    return TestClient(create_app(tmp_path)), tmp_path


def test_the_catalog_describes_every_runnable_node_type():
    assert set(CATALOG) == set(REGISTRY)


def test_node_types_and_health(client):
    http, _ = client
    assert http.get("/api/health").json() == {"status": "ok"}
    assert {t["type"] for t in http.get("/api/node-types").json()} == set(REGISTRY)


def test_validate_reports_problems_and_malformed_bodies_are_422(client):
    http, _ = client
    assert http.post("/api/pipelines/validate", json=PIPELINE).json() == {"valid": True, "problems": []}
    broken = {**PIPELINE, "edges": [{"source": "in", "target": "ghost"}]}
    assert http.post("/api/pipelines/validate", json=broken).json()["valid"] is False
    assert http.post("/api/pipelines/validate", json={"nodes": [{"type": "source.file"}]}).status_code == 422


def test_run_executes_inside_the_workspace(client):
    http, root = client
    report = http.post("/api/pipelines/run", json=PIPELINE).json()
    assert report["status"] == "ok" and (root / "out.csv").exists()


def test_run_refuses_paths_outside_the_workspace(client):
    http, root = client
    escape = {**PIPELINE, "nodes": [{"id": "in", "type": "source.file", "config": {"path": "../../etc/passwd"}}, PIPELINE["nodes"][1]]}
    report = http.post("/api/pipelines/run", json=escape).json()
    assert report["status"] == "failed" and "outside" in report["nodes"][0]["message"]


def test_run_does_not_read_the_environment(client, monkeypatch):
    http, root = client
    monkeypatch.setenv("SECRET_THING", "leak")
    pipeline = {**PIPELINE, "nodes": [{"id": "in", "type": "source.file", "config": {"path": "${SECRET_THING}.csv"}}, PIPELINE["nodes"][1]]}
    report = http.post("/api/pipelines/run", json=pipeline).json()
    assert report["status"] == "failed" and "leak" not in str(report)


@pytest.mark.parametrize("url", ["sqlite:////etc/passwd", "sqlite:///../x.db", "sqlite:///a.db?mode=ro&uri=true", "sqlite://"])
def test_sqlite_files_outside_the_workspace_are_refused(client, url):
    http, _ = client
    pipeline = {**PIPELINE, "nodes": [{"id": "in", "type": "source.database", "config": {"connection_string": url, "query": "select 1"}},
                                      PIPELINE["nodes"][1]]}
    assert http.post("/api/pipelines/run", json=pipeline).status_code == 422


def test_sqlite_inside_the_workspace_works_with_relative_urls(client):
    http, root = client
    write_database(pd.DataFrame({"x": [5]}), {"connection_string": f"sqlite:///{root / 'w.db'}", "table_name": "t"})
    pipeline = {**PIPELINE, "nodes": [{"id": "in", "type": "source.database", "config": {"connection_string": "sqlite:///w.db", "query": "select * from t"}},
                                      PIPELINE["nodes"][1]]}
    assert http.post("/api/pipelines/run", json=pipeline).json()["status"] == "ok"


def test_pentaho_import_parses_in_memory_and_limits_size(client):
    http, root = client
    ktr = b"<transformation><info><name>t</name></info><order/></transformation>"
    answer = http.post("/api/pentaho/import", content=ktr).json()
    assert answer["pipeline"]["name"] == "t" and answer["report"]["complete"] is True
    assert http.post("/api/pentaho/import", content=b"<html/>").status_code == 422
    assert http.post("/api/pentaho/import", content=b"x" * (5 * 1024 * 1024 + 1)).status_code == 413
    assert list(root.glob("*.ktr")) == []


def test_save_list_read_delete_and_name_rules(client):
    http, root = client
    assert http.put("/api/pipelines/mine", json=PIPELINE).json()["saved"] == "mine"
    assert http.get("/api/pipelines").json() == {"pipelines": ["mine"]}
    assert http.get("/api/pipelines/mine").json()["name"] == "p"
    assert http.delete("/api/pipelines/mine").status_code == 200
    assert http.get("/api/pipelines/mine").status_code == 404
    for bad in ("..%2Fescape", ".hidden", "a b"):
        assert http.put(f"/api/pipelines/{bad}", json=PIPELINE).status_code in (400, 404, 422)
    assert not (root.parent / "escape.yaml").exists()


def test_a_token_is_required_when_configured(tmp_path, monkeypatch):
    monkeypatch.setenv("BOCAGRANDE_API_TOKEN", "t0ken")
    http = TestClient(create_app(tmp_path))
    assert http.get("/api/health").status_code == 401
    assert http.get("/api/health", headers={"Authorization": "Bearer wrong"}).status_code == 401
    assert http.get("/api/health", headers={"Authorization": "Bearer t0ken"}).status_code == 200
