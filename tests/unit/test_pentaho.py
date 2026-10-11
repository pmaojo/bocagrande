import json
import sqlite3

import pandas as pd
import pytest

from adapter.db_io import read_database, write_database
from bocagrande.pentaho import PentahoParseError, import_file, import_kjb, import_ktr
from bocagrande.pentaho import cli as pentaho_cli
from bocagrande.pipeline import Context, Node, Pipeline, run_pipeline
from bocagrande.pipeline.model import Edge


# built at run time so no password-looking literal sits in the repository
CANARY = "-".join(["canary", "value", "123"])


def connection(name="db", kind="SQLITE", database=":memory:", user="", password=CANARY):
    return f"""<connection><name>{name}</name><server>localhost</server><type>{kind}</type><database>{database}</database>
    <port>5432</port><username>{user}</username><password>Encrypted {password}</password></connection>"""


def ktr(*parts, hops=(), name="demo", conn=None, params=""):
    order = "".join(f"<hop><from>{a}</from><to>{b}</to><enabled>{e}</enabled></hop>" for a, b, e in
                    [(h + ("Y",))[:3] for h in hops])
    return (f"<transformation><info><name>{name}</name><description>d</description><parameters>{params}</parameters></info>"
            f"{conn if conn is not None else connection()}<order>{order}</order>{''.join(parts)}</transformation>")


def step(name, kind, body="", x=10, y=20):
    return f"<step><name>{name}</name><type>{kind}</type>{body}<GUI><xloc>{x}</xloc><yloc>{y}</yloc></GUI></step>"


def table_input(name="In", sql="SELECT 1", connection_name="db"):
    return step(name, "TableInput", f"<connection>{connection_name}</connection><sql>{sql}</sql>")


# ---------------------------------------------------------------- converting
def test_a_database_to_database_transformation_converts_and_runs(tmp_path):
    source = tmp_path / "src.db"
    target = tmp_path / "out.db"
    write_database(pd.DataFrame({"id": [1, 2, 3, 4], "nombre": ["Ana", "Luis", "Eva", "Iván"], "edad": [30, 17, 45, 22]}),
                   {"connection_string": f"sqlite:///{source}", "table_name": "clientes"})
    xml = ktr(
        step("Leer", "TableInput", "<connection>src</connection><sql>SELECT id, nombre, edad FROM clientes</sql>", 10, 10),
        step("Campos", "SelectValues", "<fields><field><name>id</name><rename>cliente_id</rename></field>"
                                       "<field><name>nombre</name></field><field><name>edad</name></field></fields>"),
        step("Adultos", "FilterRows", "<send_true_to/><send_false_to/><compare><condition><negated>N</negated>"
                                      "<leftvalue>edad</leftvalue><function>&gt;=</function><rightvalue/>"
                                      "<value><name>constant</name><type>Integer</type><text>18</text></value></condition></compare>"),
        step("Orden", "SortRows", "<fields><field><name>edad</name><ascending>N</ascending></field></fields>"),
        step("Escribir", "TableOutput", "<connection>dst</connection><table>adultos</table><truncate>Y</truncate>"
                                        "<specify_fields>Y</specify_fields><fields><field><column_name>id_cliente</column_name>"
                                        "<stream_name>cliente_id</stream_name></field><field><column_name>nombre</column_name>"
                                        "<stream_name>nombre</stream_name></field></fields>"),
        hops=[("Leer", "Campos"), ("Campos", "Adultos"), ("Adultos", "Orden"), ("Orden", "Escribir")],
        conn=connection("src", database=source) + connection("dst", database=target))
    pipeline, report = import_ktr(xml)
    assert report.complete and report.warnings == [] and pipeline.validate() == []
    # make the target exist so the truncate has something to clear
    write_database(pd.DataFrame({"id_cliente": [99], "nombre": ["viejo"]}), {"connection_string": f"sqlite:///{target}", "table_name": "adultos"})
    result = run_pipeline(pipeline, Context(base_dir=tmp_path))
    assert [n.status for n in result.nodes] == ["ok"] * len(result.nodes), result.to_dict()
    out = read_database({"connection_string": f"sqlite:///{target}", "table_name": "adultos"})
    assert out["nombre"].tolist() == ["Eva", "Ana", "Iván"] and out["id_cliente"].tolist() == [3, 1, 4]


def test_two_csv_files_joined_and_written_as_text(tmp_path):
    pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]}).to_csv(tmp_path / "left.csv", index=False)
    pd.DataFrame({"id": [1, 3], "score": [10, 30]}).to_csv(tmp_path / "right.csv", index=False)
    xml = ktr(
        step("L", "CsvInput", "<filename>left.csv</filename><separator>,</separator><header>Y</header>"),
        step("R", "CsvInput", "<filename>right.csv</filename><separator>,</separator><header>Y</header>"),
        step("Join", "MergeJoin", "<join_type>LEFT OUTER</join_type><step1>L</step1><step2>R</step2>"
                                  "<keys_1><key>id</key></keys_1><keys_2><key>id</key></keys_2>"),
        step("Out", "TextFileOutput", "<file><name>joined</name><extention>csv</extention></file>"
                                      "<separator>;</separator><header>Y</header>"),
        hops=[("R", "Join"), ("L", "Join"), ("Join", "Out")])
    pipeline, report = import_ktr(xml)
    assert report.complete and pipeline.validate() == []
    handles = {e.source: e.handle for e in pipeline.edges if e.target == "join"}
    assert handles == {"l": "left", "r": "right"}
    result = run_pipeline(pipeline, Context(base_dir=tmp_path))
    assert all(n.status == "ok" for n in result.nodes), result.to_dict()
    written = pd.read_csv(tmp_path / "joined.csv", sep=";")
    assert written["id"].tolist() == [1, 2, 3] and written["score"].tolist()[0] == 10 and pd.isna(written["score"][1])


def test_calculator_and_constant_run(tmp_path):
    xml = ktr(
        step("Fila", "TableInput", "<connection>db</connection><sql>SELECT 2 AS a, 3 AS b</sql>"),
        step("Calc", "Calculator", "<calculation><field_name>suma</field_name><calc_type>ADD</calc_type>"
                                   "<field_a>a</field_a><field_b>b</field_b></calculation>"
                                   "<calculation><field_name>copia</field_name><calc_type>COPY_OF_FIELD</calc_type>"
                                   "<field_a>suma</field_a></calculation>"),
        step("Fijo", "Constant", "<fields><field><name>origen</name><type>String</type><nullif>ktr</nullif></field></fields>"),
        hops=[("Fila", "Calc"), ("Calc", "Fijo")], conn=connection(database=tmp_path / "x.db"))
    pipeline, report = import_ktr(xml)
    assert report.complete
    pipeline.nodes[0].config["connection_string"] = f"sqlite:///{tmp_path / 'x.db'}"
    sqlite3.connect(tmp_path / "x.db").close()
    result = run_pipeline(pipeline, Context(base_dir=tmp_path))
    assert all(n.status == "ok" for n in result.nodes), result.to_dict()


def test_unsupported_steps_stay_visible_and_are_reported():
    xml = ktr(table_input(), step("Script", "ScriptValueMod", "<jsScripts>var x = 1;</jsScripts><password>PW</password>".replace("PW", CANARY)),
              hops=[("In", "Script")])
    pipeline, report = import_ktr(xml)
    assert not report.complete and report.unsupported[0]["step"] == "Script"
    node = pipeline.nodes[1]
    assert node.type == "unsupported.ScriptValueMod" and "password" not in node.config and CANARY not in json.dumps(node.config) and node.config["reason"]
    assert pipeline.validate() == []
    ran = run_pipeline(pipeline, Context())
    assert ran.nodes[1].status != "ok"


@pytest.mark.parametrize("body, reason", [
    ("<send_true_to>A</send_true_to><send_false_to>B</send_false_to><compare><condition/></compare>", "separate outputs"),
    ("<compare><condition><leftvalue>x</leftvalue><function>IS NULL</function></condition></compare>", "IS NULL"),
])
def test_filters_that_cannot_be_expressed_are_not_approximated(body, reason):
    _, report = import_ktr(ktr(table_input(), step("F", "FilterRows", body), hops=[("In", "F")]))
    assert reason in report.unsupported[0]["reason"]


def test_a_connection_password_is_never_read_and_becomes_an_environment_reference():
    xml = ktr(table_input(), conn=connection(kind="POSTGRESQL", database="dw", user="etl"))
    pipeline, report = import_ktr(xml)
    url = pipeline.nodes[0].config["connection_string"]
    assert url == "postgresql+psycopg://etl:${DB_PASSWORD}@localhost:5432/dw"
    assert report.environment == ["DB_PASSWORD"]
    assert CANARY not in json.dumps(pipeline.to_dict()) and CANARY not in json.dumps(report.to_dict())


def test_an_unknown_database_type_is_reported_not_guessed():
    _, report = import_ktr(ktr(table_input(), conn=connection(kind="INFORMIX")))
    assert "INFORMIX" in report.unsupported[0]["reason"]


def test_disabled_and_dangling_hops_are_warned():
    _, report = import_ktr(ktr(table_input(), step("D", "Dummy"), hops=[("In", "D", "N"), ("In", "Ghost")]))
    assert len(report.warnings) == 2


def test_duplicate_step_names_after_slugging_get_unique_ids():
    pipeline, _ = import_ktr(ktr(table_input("A b"), table_input("a-b"), table_input("a_b")))
    assert len({n.id for n in pipeline.nodes}) == 3


# ---------------------------------------------------------------- jobs
KJB = """<job><name>carga</name><description>nocturna</description><entries>
<entry><name>START</name><type>SPECIAL</type><xloc>10</xloc><yloc>10</yloc></entry>
<entry><name>Cargar</name><type>TRANS</type><filename>${Internal.Job.Filename.Directory}/cargar.ktr</filename><xloc>100</xloc><yloc>10</yloc></entry>
<entry><name>cargar</name><type>MAIL</type><xloc>200</xloc><yloc>10</yloc><password>PW</password></entry>
</entries><hops>
<hop><from>START</from><to>Cargar</to><enabled>Y</enabled><unconditional>Y</unconditional></hop>
<hop><from>Cargar</from><to>cargar</to><enabled>Y</enabled><evaluation>N</evaluation><unconditional>N</unconditional></hop>
</hops></job>""".replace("PW", CANARY)


def test_a_job_becomes_an_outline_with_unique_ids():
    pipeline, report = import_kjb(KJB)
    assert [n.id for n in pipeline.nodes] == ["start", "cargar", "cargar_2"]
    assert all(n.type.startswith("unsupported.job.") for n in pipeline.nodes)
    assert len(pipeline.edges) == 2 and pipeline.validate() == []
    assert len(report.unsupported) == 3 and any("failure" in w for w in report.warnings)
    assert CANARY not in json.dumps(pipeline.to_dict())


def test_import_file_decides_by_root_element_and_refuses_others():
    assert import_file(KJB)[0].name == "carga"
    assert import_file(ktr(table_input()))[0].name == "demo"
    with pytest.raises(PentahoParseError):
        import_file("<html/>")


# ---------------------------------------------------------------- hostile input
BILLION_LAUGHS = ('<?xml version="1.0"?><!DOCTYPE l [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;&a;&a;">]>'
                  '<transformation><info><name>&b;</name></info></transformation>')
XXE = ('<?xml version="1.0"?><!DOCTYPE t [<!ENTITY x SYSTEM "file:///etc/passwd">]>'
       '<transformation><info><name>&x;</name></info></transformation>')


@pytest.mark.parametrize("payload", [BILLION_LAUGHS, XXE])
def test_entities_are_refused(payload):
    with pytest.raises(PentahoParseError):
        import_ktr(payload)


@pytest.mark.parametrize("payload", ["", "not xml at all <", b"\xff\xfe"])
def test_garbage_is_a_parse_error(payload):
    with pytest.raises(PentahoParseError):
        import_ktr(payload)


# ---------------------------------------------------------------- command line
def test_the_command_line_writes_a_pipeline_file_and_signals_incomplete_imports(tmp_path, capsys):
    complete = tmp_path / "a.ktr"
    complete.write_text(ktr(table_input()), encoding="utf-8")
    out = tmp_path / "a.yaml"
    assert pentaho_cli.main([str(complete), "-o", str(out)]) == 0
    assert Pipeline.from_yaml(out.read_text(encoding="utf-8")).validate() == []

    partial = tmp_path / "b.ktr"
    partial.write_text(ktr(table_input(), step("S", "ScriptValueMod"), hops=[("In", "S")]), encoding="utf-8")
    assert pentaho_cli.main([str(partial), "-o", str(tmp_path / "b.json")]) == 1
    assert "not converted: S" in capsys.readouterr().err
    assert pentaho_cli.main([str(tmp_path / "missing.ktr")]) == 2


# ---------------------------------------------------------------- pieces added to the pipeline engine
def test_truncate_clears_the_table_and_keeps_it(tmp_path):
    url = f"sqlite:///{tmp_path / 't.db'}"
    write_database(pd.DataFrame({"a": [1, 2]}), {"connection_string": url, "table_name": "t"})
    write_database(pd.DataFrame({"a": [9]}), {"connection_string": url, "table_name": "t", "if_exists": "truncate"})
    assert read_database({"connection_string": url, "table_name": "t"})["a"].tolist() == [9]


def test_compute_adds_columns_in_order_and_rejects_unsafe_formulas(tmp_path):
    source = Node("s", "source.database", {"connection_string": f"sqlite:///{tmp_path / 'c.db'}", "query": "SELECT 2 AS a, 3 AS b"})
    sqlite3.connect(tmp_path / "c.db").close()
    good = Pipeline(nodes=[source, Node("c", "transform.compute", {"columns": [{"name": "s", "formula": "a + b"}, {"name": "d", "formula": "s * 2"}]})],
                    edges=[Edge("s", "c")])
    assert all(n.status == "ok" for n in run_pipeline(good, Context(base_dir=tmp_path)).nodes)
    bad = Pipeline(nodes=[source, Node("c", "transform.compute", {"columns": [{"name": "s", "formula": "__import__('os')"}]})],
                   edges=[Edge("s", "c")])
    assert run_pipeline(bad, Context(base_dir=tmp_path)).nodes[1].status == "failed"
