import json

import pandas as pd
import pytest

from adapter.db_io import read_database, write_database
from bocagrande.pipeline import Context, Edge, Node, Pipeline, PipelineError, load_pipeline, run_pipeline
from bocagrande.pipeline import cli as pipeline_cli


def node(node_id, type_, **config):
    return Node(id=node_id, type=type_, config=config)


def chain(*nodes, **kwargs):
    edges = [Edge(a.id, b.id) for a, b in zip(nodes, nodes[1:])]
    return Pipeline(name="t", nodes=list(nodes), edges=edges, **kwargs)


@pytest.fixture
def workspace(tmp_path):
    url = f"sqlite:///{tmp_path / 'src.db'}"
    write_database(pd.DataFrame({"id": [1, 2, 3, 4], "nombre": ["Ana", "Luis", "Eva", "Iván"], "edad": [30, 17, 45, 22],
                                 "pais": ["ES", "ES", "FR", "ES"]}), {"connection_string": url, "table_name": "clientes"})
    return tmp_path, url


# ---------------------------------------------------------------- model
def test_a_valid_pipeline_round_trips_through_yaml_and_json():
    pipeline = chain(node("a", "source.file", path="x.csv"), node("b", "transform.sort", by=[{"column": "id"}]),
                     node("c", "destination.file", path="y.csv"), parameters={"P": "1"})
    again = Pipeline.from_yaml(pipeline.to_yaml())
    assert again.to_dict() == pipeline.to_dict() and again.validate() == []
    assert Pipeline.from_dict(json.loads(json.dumps(pipeline.to_dict()))).to_dict() == pipeline.to_dict()


def test_execution_order_follows_the_edges():
    pipeline = Pipeline(nodes=[node("c", "destination.file"), node("a", "source.file"), node("b", "transform.sort")],
                        edges=[Edge("b", "c"), Edge("a", "b")])
    assert [n.id for n in pipeline.execution_order()] == ["a", "b", "c"]


@pytest.mark.parametrize("build, expected", [
    (lambda: Pipeline(nodes=[node("a", "source.file"), node("a", "source.file")]), "used more than once"),
    (lambda: Pipeline(nodes=[node("a", "source.file")], edges=[Edge("a", "ghost")]), "unknown node 'ghost'"),
    (lambda: Pipeline(nodes=[node("a", "weird.thing")]), "must start with one of"),
    (lambda: Pipeline(nodes=[node("a", "source.file"), node("b", "source.file")], edges=[Edge("a", "b")]), "cannot have an input"),
    (lambda: Pipeline(nodes=[node("a", "destination.file"), node("b", "destination.file")], edges=[Edge("a", "b")]), "cannot have an output"),
    (lambda: Pipeline(nodes=[node("t", "transform.sort")]), "needs exactly one input"),
    (lambda: Pipeline(nodes=[node("a", "source.file"), node("b", "source.file"), node("j", "transform.merge_join")],
                      edges=[Edge("a", "j"), Edge("b", "j")]), "'left' and one on 'right'"),
    (lambda: Pipeline(nodes=[node("x", "transform.sort"), node("y", "transform.sort")], edges=[Edge("x", "y"), Edge("y", "x")]), "cycle"),
])
def test_validation_reports_each_problem(build, expected):
    assert any(expected in p for p in build().validate())


def test_malformed_input_is_a_pipeline_error_not_a_crash():
    with pytest.raises(PipelineError):
        Pipeline.from_dict({"nodes": [{"type": "source.file"}]})
    with pytest.raises(PipelineError):
        Pipeline.from_yaml("just a string")
    with pytest.raises(PipelineError):
        Pipeline.from_yaml("a: [unclosed")


# ---------------------------------------------------------------- running
def test_database_to_database_with_select_filter_sort(workspace):
    tmp, url = workspace
    pipeline = chain(
        node("src", "source.database", connection_string=url, table_name="clientes"),
        node("sel", "transform.select", columns=[{"name": "id", "rename": "codigo"}, {"name": "nombre"}, {"name": "edad"}]),
        node("adult", "transform.filter", condition="edad >= 18"),
        node("sort", "transform.sort", by=[{"column": "edad", "ascending": False}]),
        node("dst", "destination.database", connection_string=url, table_name="adultos", if_exists="replace"),
    )
    report = run_pipeline(pipeline, Context(base_dir=tmp))
    assert report.status == "ok", report.to_dict()
    out = read_database({"connection_string": url, "table_name": "adultos"})
    assert list(out.columns) == ["codigo", "nombre", "edad"] and out["nombre"].tolist() == ["Eva", "Ana", "Iván"]
    assert [(n.id, n.rows) for n in report.nodes][:3] == [("src", 4), ("sel", 4), ("adult", 3)]


def test_file_to_file_formats(tmp_path):
    (tmp_path / "in.csv").write_text("a;b\n1;x\n2;y\n")
    pipeline = chain(node("s", "source.file", path="in.csv", options={"sep": ";"}),
                     node("c", "transform.constant", fields=[{"name": "k", "value": "K"}]),
                     node("d", "destination.file", path="out/result.json"))
    assert run_pipeline(pipeline, Context(base_dir=tmp_path)).status == "ok"
    assert json.loads((tmp_path / "out" / "result.json").read_text()) == [
        {"a": 1, "b": "x", "k": "K"}, {"a": 2, "b": "y", "k": "K"}]


def test_text_columns_keep_leading_zeros(tmp_path):
    (tmp_path / "in.csv").write_text("tienda,n\n001,1\n002,2\n")
    pipeline = chain(node("s", "source.file", path="in.csv", text_columns=["tienda"]), node("d", "destination.file", path="o.csv"))
    run_pipeline(pipeline, Context(base_dir=tmp_path))
    assert (tmp_path / "o.csv").read_text().splitlines()[1] == "001,1"


def test_mapping_node_applies_steps_and_screens_formulas(tmp_path):
    (tmp_path / "in.csv").write_text("a,b\n1,2\n3,4\n")
    pipeline = chain(
        node("s", "source.file", path="in.csv"),
        node("m", "transform.mapping", steps=[
            {"campo_salida": "x", "campo_entrada": "a"},
            {"campo_salida": "suma", "tipo_transformacion": "formula", "formula": "a + b"},
            {"campo_salida": "malo", "tipo_transformacion": "formula", "formula": "__import__('os').getcwd()"}]),
        node("d", "destination.file", path="o.csv"))
    report = run_pipeline(pipeline, Context(base_dir=tmp_path))
    assert report.status == "ok" and "1 missing (malo)" in report.nodes[1].message
    assert (tmp_path / "o.csv").read_text().splitlines()[1] == "1,3,"


def test_merge_join_uses_left_and_right_inputs(tmp_path):
    (tmp_path / "l.csv").write_text("id,v\n1,a\n2,b\n")
    (tmp_path / "r.csv").write_text("k,w\n2,x\n3,y\n")
    pipeline = Pipeline(
        nodes=[node("l", "source.file", path="l.csv"), node("r", "source.file", path="r.csv"),
               node("j", "transform.merge_join", keys_left=["id"], keys_right=["k"], how="left"),
               node("d", "destination.file", path="o.csv")],
        edges=[Edge("l", "j", "left"), Edge("r", "j", "right"), Edge("j", "d")])
    assert run_pipeline(pipeline, Context(base_dir=tmp_path)).status == "ok"
    rows = (tmp_path / "o.csv").read_text().splitlines()
    assert rows[0] == "id,v,k,w" and rows[1] == "1,a,," and rows[2] == "2,b,2,x"


def test_a_failing_node_skips_what_depends_on_it_but_not_other_branches(tmp_path):
    (tmp_path / "in.csv").write_text("a\n1\n")
    pipeline = Pipeline(
        nodes=[node("s", "source.file", path="in.csv"), node("bad", "transform.sort", by=[{"column": "nope"}]),
               node("after", "destination.file", path="bad.csv"), node("ok", "destination.file", path="good.csv")],
        edges=[Edge("s", "bad"), Edge("bad", "after"), Edge("s", "ok")])
    report = run_pipeline(pipeline, Context(base_dir=tmp_path))
    status = {n.id: n.status for n in report.nodes}
    assert status == {"s": "ok", "bad": "failed", "after": "skipped", "ok": "ok"} and report.status == "failed"
    assert "nope" in report.failed[0].message and "Available: a" in report.failed[0].message
    assert (tmp_path / "good.csv").exists() and not (tmp_path / "bad.csv").exists()


def test_unsupported_nodes_are_kept_but_do_not_run(tmp_path):
    (tmp_path / "in.csv").write_text("a\n1\n")
    pipeline = chain(node("s", "source.file", path="in.csv"), node("u", "unsupported.ScriptValueMod", script="..."),
                     node("d", "destination.file", path="o.csv"))
    report = run_pipeline(pipeline, Context(base_dir=tmp_path))
    assert [n.status for n in report.nodes] == ["ok", "failed", "skipped"] and "not supported" in report.nodes[1].message


def test_an_invalid_pipeline_is_refused_before_anything_runs(tmp_path):
    with pytest.raises(PipelineError, match="Invalid pipeline"):
        run_pipeline(Pipeline(nodes=[node("t", "transform.sort")]), Context(base_dir=tmp_path))


def test_filter_conditions_are_screened(tmp_path):
    (tmp_path / "in.csv").write_text("a\n1\n")
    pipeline = chain(node("s", "source.file", path="in.csv"),
                     node("f", "transform.filter", condition="__import__('os').system('id') == 0"),
                     node("d", "destination.file", path="o.csv"))
    report = run_pipeline(pipeline, Context(base_dir=tmp_path))
    assert report.nodes[1].status == "failed" and "rejected" in report.nodes[1].message


# ---------------------------------------------------------------- parameters, secrets, sandbox
def test_parameters_fill_in_values_and_unknown_references_fail(tmp_path):
    (tmp_path / "in.csv").write_text("a\n1\n")
    pipeline = chain(node("s", "source.file", path="${FILE}"), node("d", "destination.file", path="o.csv"),
                     parameters={"FILE": "in.csv"})
    assert run_pipeline(pipeline, Context(base_dir=tmp_path)).status == "ok"
    pipeline.nodes[0].config["path"] = "${MISSING}"
    report = run_pipeline(pipeline, Context(base_dir=tmp_path))
    assert report.nodes[0].status == "failed" and "not a parameter" in report.nodes[0].message


def test_environment_variables_are_only_read_in_connection_strings(tmp_path, monkeypatch):
    monkeypatch.setenv("BG_SECRET", "s3cret-value")
    (tmp_path / "in.csv").write_text("a\n1\n")
    leaky = chain(node("s", "source.file", path="${BG_SECRET}"), node("d", "destination.file", path="o.csv"))
    report = run_pipeline(leaky, Context(base_dir=tmp_path))
    assert report.nodes[0].status == "failed" and "s3cret-value" not in report.nodes[0].message

    db = tmp_path / "e.db"
    monkeypatch.setenv("BG_DB", str(db))
    write_database(pd.DataFrame({"a": [1]}), {"connection_string": f"sqlite:///{db}", "table_name": "t"})
    pipeline = chain(node("s", "source.database", connection_string="sqlite:///${BG_DB}", table_name="t"),
                     node("d", "destination.file", path="o.csv"))
    assert run_pipeline(pipeline, Context(base_dir=tmp_path)).status == "ok"
    refused = run_pipeline(pipeline, Context(base_dir=tmp_path, allow_env=False))
    assert refused.nodes[0].status == "failed"


def test_sandbox_keeps_paths_inside_the_base_directory(tmp_path):
    base = tmp_path / "work"
    base.mkdir()
    (tmp_path / "outside.csv").write_text("a\n1\n")
    pipeline = chain(node("s", "source.file", path="../outside.csv"), node("d", "destination.file", path="o.csv"))
    assert run_pipeline(pipeline, Context(base_dir=base, sandbox=False)).nodes[0].status == "ok"
    report = run_pipeline(pipeline, Context(base_dir=base, sandbox=True))
    assert report.nodes[0].status == "failed" and "outside the working directory" in report.nodes[0].message
    absolute = chain(node("s", "source.file", path=str(tmp_path / "outside.csv")), node("d", "destination.file", path="o.csv"))
    assert run_pipeline(absolute, Context(base_dir=base, sandbox=True)).nodes[0].status == "failed"


# ---------------------------------------------------------------- graph destination and CLI
GRAPH_SCHEMA = "table: CLIENTES\ncolumns:\n  - name: id\n    type: integer\n    required: true\n  - name: nombre\n    type: string\n    length: 10\n"


def test_destination_graph_writes_turtle_and_can_fail_on_shacl_violations(workspace):
    tmp, url = workspace
    (tmp / "clientes.yaml").write_text(GRAPH_SCHEMA)
    base = [node("src", "source.database", connection_string=url, table_name="clientes"),
            node("sel", "transform.select", columns=[{"name": "id"}, {"name": "nombre"}])]
    good = chain(*base, node("g", "destination.graph", schema="clientes.yaml", output="out/g.ttl", reasoner="ontologos", validate=True))
    report = run_pipeline(good, Context(base_dir=tmp))
    assert report.status == "ok", report.to_dict()
    assert "consistent" in report.nodes[-1].message and "SHACL conforms" in report.nodes[-1].message
    assert '"Ana"' in (tmp / "out" / "g.ttl").read_text()

    (tmp / "clientes.yaml").write_text(GRAPH_SCHEMA.replace("length: 10", "length: 2"))
    strict = chain(*base, node("g", "destination.graph", schema="clientes.yaml", output="out/s.ttl", reasoner="none",
                               validate=True, fail_on_violations=True))
    failed = run_pipeline(strict, Context(base_dir=tmp))
    assert failed.nodes[-1].status == "failed" and "SHACL validation failed" in failed.nodes[-1].message
    assert not (tmp / "out" / "s.ttl").exists()


def test_cli_checks_and_runs_a_pipeline_file(tmp_path, capsys):
    (tmp_path / "in.csv").write_text("a\n1\n2\n")
    path = tmp_path / "p.yaml"
    path.write_text(chain(node("s", "source.file", path="in.csv"), node("d", "destination.file", path="${OUT}"),
                          parameters={"OUT": "o.csv"}).to_yaml())
    pipeline_cli.main([str(path), "--check"])
    assert "valid (2 nodes, 1 edges)" in capsys.readouterr().out
    pipeline_cli.main([str(path), "--param", "OUT=custom.csv", "--sandbox"])
    assert "[ok     ] d (destination.file)" in capsys.readouterr().out and (tmp_path / "custom.csv").exists()
    assert load_pipeline(path).name == "t"


def test_cli_exits_non_zero_when_a_node_fails_or_the_file_is_invalid(tmp_path):
    (tmp_path / "bad.yaml").write_text(chain(node("s", "source.file", path="missing.csv"), node("d", "destination.file", path="o.csv")).to_yaml())
    with pytest.raises(SystemExit) as failing:
        pipeline_cli.main([str(tmp_path / "bad.yaml")])
    assert failing.value.code == 1
    (tmp_path / "broken.yaml").write_text("nodes: [{id: a, type: transform.sort}]")
    with pytest.raises(SystemExit) as invalid:
        pipeline_cli.main([str(tmp_path / "broken.yaml")])
    assert "Invalid pipeline" in str(invalid.value)
    with pytest.raises(SystemExit) as absent:
        pipeline_cli.main([str(tmp_path / "nope.yaml")])
    assert "Could not load" in str(absent.value)
