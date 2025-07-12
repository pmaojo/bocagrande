from ontology.model import PropertyDef, ClassDef, Individual, TableSchema


def test_property_def_defaults():
    prop = PropertyDef(name="foo")
    assert prop.name == "foo"
    assert prop.tipo == "string"
    assert prop.requerido is False
    assert prop.length is None
    assert prop.formato is None
    assert prop.metadata == {}


def test_class_def():
    c = ClassDef(name="Person", properties=[PropertyDef(name="age")])
    assert c.name == "Person"
    assert c.properties[0].name == "age"


def test_individual():
    ind = Individual(class_name="Person", values={"age": 30})
    assert ind.class_name == "Person"
    assert ind.values == {"age": 30}


def test_table_schema_defaults():
    schema = TableSchema(name="TEST", fields=[PropertyDef(name="id")])
    assert schema.name == "TEST"
    assert schema.fields[0].name == "id"
    assert schema.primary_key == []
    assert schema.metadata == {}
