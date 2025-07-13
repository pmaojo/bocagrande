from bocagrande.langchain_agent import generate_steps
from bocagrande.transform import ETLStep
from ontology.model import TableSchema, PropertyDef
from langchain_core.language_models.fake import FakeListLLM


def test_generate_steps_with_fake_llm():
    headers = ["a"]
    schema = TableSchema(name="TEST", fields=[PropertyDef(name="x")])
    llm = FakeListLLM(responses=['[{"campo_salida": "x", "campo_entrada": "a"}]'])

    steps = generate_steps(headers, schema, llm)

    assert steps == [ETLStep(campo_salida="x", campo_entrada="a")]
