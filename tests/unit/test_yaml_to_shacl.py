from ontology.model import TableSchema, PropertyDef
from adapter.yaml_to_shacl import generar_shape_shacl


def test_generar_shape_shacl_basic():
    field = PropertyDef(name="id", tipo="integer", requerido=True, length=5)
    schema = TableSchema(name="CLIENTES", fields=[field])
    turtle = generar_shape_shacl(schema)
    assert "bg:CLIENTESShape" in turtle
    assert "sh:datatype xsd:integer" in turtle
    assert "sh:minCount 1" in turtle
    assert "sh:maxLength 5" in turtle
