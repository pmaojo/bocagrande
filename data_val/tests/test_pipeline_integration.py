import os
import yaml
import difflib
from src.casos_uso import (
    caso_parsear_pdf,
    caso_validar_schema,
    caso_normalizar_schema,
    caso_serializar_yaml
)

def test_pipeline_contrato_real():
    pdf_path = 'data/input/Formato CSV trasvase de datos GIO.pdf'
    golden_path = 'tests/golden/contrato.yaml'
    output_path = 'tests/golden/tmp_salida.yaml'
    # Ejecutar pipeline real
    schema = caso_parsear_pdf(pdf_path)
    errores = caso_validar_schema(schema)
    assert not errores, f"Errores de validación: {errores}"
    # Saltar Gemini en test (o usar modo sugerencia si lo deseas)
    # schema = caso_normalizar_schema(schema, None)
    caso_serializar_yaml(schema, output_path)
    # Comparar con golden file
    with open(golden_path, encoding='utf-8') as f:
        golden = f.read().splitlines()
    with open(output_path, encoding='utf-8') as f:
        actual = f.read().splitlines()
    if golden != actual:
        diff = '\n'.join(difflib.unified_diff(golden, actual, fromfile='golden', tofile='actual', lineterm=''))
        assert False, f"La salida no coincide con el golden file:\n{diff}"
    os.remove(output_path) 