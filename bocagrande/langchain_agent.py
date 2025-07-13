"""LLM-powered helper to generate ETL steps."""
from __future__ import annotations

from dataclasses import asdict
import json
from typing import List

from langchain_core.language_models.base import BaseLanguageModel
from langchain_core.prompts import PromptTemplate
from langchain.chains.llm import LLMChain
from langchain_core.output_parsers import JsonOutputParser as JSONOutputParser

from ontology.model import TableSchema
from bocagrande.transform import ETLStep


def generate_steps(headers: List[str], schema: TableSchema, llm: BaseLanguageModel) -> List[ETLStep]:
    """Return ETL steps suggested by the language model."""

    schema_json = json.dumps(asdict(schema), ensure_ascii=False)
    prompt = PromptTemplate(
        input_variables=["headers", "schema"],
        template=(
            "You are an ETL assistant. Given the CSV headers {headers} and the "
            "target schema {schema}, return a JSON list of transformation "
            "objects with keys: campo_salida, campo_entrada, tipo_transformacion, "
            "formula and descripcion."
        ),
    )

    chain = LLMChain(llm=llm, prompt=prompt)
    result = chain.invoke({"headers": headers, "schema": schema_json})
    parser = JSONOutputParser()
    data = parser.parse(result["text"])
    return [ETLStep(**item) for item in data]


__all__ = ["generate_steps"]
