import sys
import os
from src.casos_uso import (
    caso_parsear_pdf,
    caso_validar_schema,
    caso_normalizar_schema,
    caso_serializar_yaml,
    caso_normalizar_llm,
    exportar_tablas_a_yaml
)
from src.validador_gemini import validar_tablas_con_gemini
from src.adaptador_gemini import GeminiNormalizer, batch_normalize_with_gemini

# Eliminar toda la lógica relacionada con PDF, extracción, validación y normalización LLM
# Puedes dejar este archivo vacío o solo con un mensaje deprecado si todo era para PDF

if __name__ == "__main__":
    print("Este CLI ha sido deprecado. Usa parsear_csv_formato.py para el nuevo flujo basado en CSV.") 