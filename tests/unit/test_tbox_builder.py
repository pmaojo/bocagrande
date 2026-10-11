from rdflib import RDF, RDFS, OWL, XSD, URIRef
from ontology.tbox_builder import build_global_tbox


def test_build_global_tbox_basic(tmp_path):
    yaml_content = """
fields:
  - Campo: id
    Tipo: integer
"""
    file = tmp_path / "clientes.yaml"
    file.write_text(yaml_content)
    g = build_global_tbox(str(tmp_path))
    class_uri = URIRef("http://bocagrande.local/ont#CLIENTES")
    prop_uri = URIRef("http://bocagrande.local/ont#ID")
    assert (class_uri, RDF.type, OWL.Class) in g
    assert (prop_uri, RDF.type, OWL.DatatypeProperty) in g
    assert (prop_uri, RDFS.range, XSD.integer) in g


def test_build_global_tbox_ignores_invalid_yaml(tmp_path):
    valid = tmp_path / "ok.yaml"
    valid.write_text("fields:\n  - Campo: name")
    bad = tmp_path / "bad.yaml"
    bad.write_text("foo: [bar")
    g = build_global_tbox(str(tmp_path))
    assert (URIRef("http://bocagrande.local/ont#BAD"), RDF.type, OWL.Class) not in g
    assert len(g) > 0


def test_invalid_yaml_is_logged_not_printed(tmp_path, caplog, capsys):
    (tmp_path / "bad.yaml").write_text("foo: [bar")
    with caplog.at_level("WARNING", logger="ontology.tbox_builder"):
        build_global_tbox(str(tmp_path))
    assert "bad.yaml" in caplog.text
    assert capsys.readouterr().out == ""
