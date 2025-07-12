from rdflib import Graph, RDF, RDFS, OWL, Literal, URIRef
import pandas as pd

from ontology.service import OntologyBuilder
from ontology.model import TableSchema, PropertyDef
from ontology.tbox_builder import build_global_tbox
from ontology.utils import BASE, limpiar_para_uri


def test_build_abox_graph_uses_utils(tmp_path):
    schema = TableSchema(name="Foo Bar", fields=[PropertyDef(name="full name")])
    df = pd.DataFrame({"full name": ["Alice"]})

    tbox = Graph()
    builder = OntologyBuilder(tbox)
    g = builder.build_abox_graph(schema, df)

    class_uri = BASE[limpiar_para_uri(schema.name.upper())]
    prop_uri = BASE[limpiar_para_uri("full name".upper())]
    ind_uri = BASE[f"{limpiar_para_uri(schema.name)}_1"]

    assert (ind_uri, RDF.type, class_uri) in g
    assert (ind_uri, prop_uri, Literal("Alice")) in g


def test_build_global_tbox_uses_utils(tmp_path):
    yaml_content = "fields:\n  - Campo: full name\n    Tipo: string"
    file = tmp_path / "my table.yaml"
    file.write_text(yaml_content)

    g = build_global_tbox(str(tmp_path))

    class_uri = BASE[limpiar_para_uri("MY TABLE".upper())]
    prop_uri = BASE[limpiar_para_uri("full name".upper())]

    assert (class_uri, RDF.type, OWL.Class) in g
    assert (prop_uri, RDF.type, OWL.DatatypeProperty) in g
    assert (prop_uri, RDFS.domain, class_uri) in g

