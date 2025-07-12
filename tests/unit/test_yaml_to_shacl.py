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


def test_generar_shape_shacl_precision_scale():
    field = PropertyDef(
        name="importe",
        tipo="float",
        requerido=True,
        precision=10,
        scale=2,
    )
    schema = TableSchema(name="PAGOS", fields=[field])
    turtle = generar_shape_shacl(schema)
    assert "bg:PAGOSShape" in turtle
    assert "sh:datatype xsd:float" in turtle
    assert "sh:pattern" in turtle
