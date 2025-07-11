import os
from app.pentaho_importer import (
    TransformationModel,
    Connection,
    Step,
    Hop,
    TransformationModelSchema,
)


def create_sample_model():
    model = TransformationModel(name="Test", description="desc")
    conn = Connection(
        name="c1",
        db_type="postgres",
        host="localhost",
        db_name="db",
        port="5432",
        user="user",
        password="pass",
    )
    model.add_connection(conn)
    step = Step(name="s1", step_type="TableInput")
    model.add_step(step)
    model.add_hop(Hop("s1", "s1"))
    model.parameters["p"] = "v"
    return model


def test_schema_serialization():
    model = create_sample_model()
    schema = TransformationModelSchema.model_validate(model)
    data = schema.model_dump()
    assert data["name"] == "Test"
    assert data["connections"][0]["name"] == "c1"
    assert data["steps"][0]["name"] == "s1"



def test_schema_from_endpoint(client):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
    ktr_path = os.path.join(base_dir, "docs", "pentaho_examples", "Clientes_Vivaldi.ktr")
    with open(ktr_path, "rb") as f:
        response = client.post("/api/v1/pentaho/upload-ktr/", files={"file": ("Clientes_Vivaldi.ktr", f, "text/xml")})
    assert response.status_code == 200
    TransformationModelSchema.model_validate(response.json())
