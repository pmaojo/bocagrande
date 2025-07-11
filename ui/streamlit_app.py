"""
Interfaz: UI Streamlit para orquestar el pipeline semántico.
"""
import tempfile
import streamlit as st
import sys
import os
import asyncio # Importar asyncio
from typing import Tuple # Importar Tuple
import pandas as pd # Añadir import para pandas
import numpy as np
import traceback
import google.generativeai as genai # Añadir import para Gemini
import json # Mover import json aquí para que esté siempre disponible
import re

# Añadir la raíz del proyecto al sys.path para asegurar la detección de módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ontology.service import OntologyBuilder
from adapter.yaml_loader import load_schema
from adapter.csv_loader import read_csv
from adapter.hermit_runner import reason_async
from adapter.shacl_runner import validate_shacl
from adapter.yaml_to_shacl import generar_shape_shacl
from ontology.tbox_builder import build_global_tbox

# Estructura básica de la UI

# Función síncrona para ejecutar la coroutine de HermiT
def _run_hermit_sync(owl_path: str) -> Tuple[bool, str]:
    return asyncio.run(reason_async(owl_path))

# Cargar la TBox global al inicio de la aplicación (se cachea con @st.cache_resource)
@st.cache_resource
def get_global_tbox():
    return build_global_tbox()

def main():
    """
    Orquesta la carga de archivos, comparación, prueba y aplicación de transformaciones.
    """
    st.title("Conversor Semántico Universal ")
    st.sidebar.info("Flujo: Comparación, prueba y transformación de datos.")

    # --- 1. Comparación y Generación de Transformaciones ---
    st.header("1. Comparación y Generación de Transformaciones")
    from pathlib import Path
    import re
    archivo_muestra_comparacion = st.file_uploader("Sube un archivo de muestra para comparación (CSV)", type=["csv"], key="comparacion_data")
    esquemas_disponibles_comparacion = list(Path("schema_yaml").glob("*.yaml"))

    # Autoselección de esquema YAML para comparación
    def sugerir_esquema_yaml(archivo, esquemas):
        if not archivo:
            return None
        nombre_base = Path(archivo.name).stem
        nombre_base = re.sub(r"(_sample|_muestra|_datos|_data)$", "", nombre_base, flags=re.IGNORECASE)
        for esquema in esquemas:
            nombre_yaml = esquema.stem
            nombre_yaml = re.sub(r"\.metadatos$", "", nombre_yaml, flags=re.IGNORECASE)
            if nombre_yaml.lower() == nombre_base.lower():
                return esquema
        return None

    if archivo_muestra_comparacion:
        sugerido = sugerir_esquema_yaml(archivo_muestra_comparacion, esquemas_disponibles_comparacion)
        if sugerido and (not st.session_state.get("comparacion_schema") or st.session_state["comparacion_schema"] != sugerido):
            st.session_state["comparacion_schema"] = sugerido
            st.info(f"Esquema YAML sugerido automáticamente para comparación: {sugerido.name}")
        elif not sugerido:
            st.warning("No se encontró un esquema YAML que coincida con el archivo de muestra subido. Selecciónalo manualmente.")

    esquema_destino_comparacion = st.selectbox("Selecciona un esquema YAML para comparar", esquemas_disponibles_comparacion, key="comparacion_schema")

    if archivo_muestra_comparacion and esquema_destino_comparacion:
        st.info("Archivo de muestra y esquema de destino seleccionados para comparación.")
        transformaciones_sugeridas = st.session_state.get('transformaciones_sugeridas', [])
        if st.button("Generar Transformaciones Sugeridas con IA"):
            df_muestra = pd.read_csv(archivo_muestra_comparacion)
            headers_csv = df_muestra.columns.tolist()
            st.write("Campos de entrada (CSV):", headers_csv)

            tabla_schema_destino = load_schema(str(esquema_destino_comparacion))

            if tabla_schema_destino is None:
                st.error(f"Error: El esquema '{esquema_destino_comparacion.name}' no tiene un formato válido.")
            else:
                campos_destino = [campo.name for campo in tabla_schema_destino.fields]
                st.write("Campos de salida (esquema YAML):", campos_destino)

                st.subheader("Sugerencias de Transformación (Generadas por IA)")
                st.info("Conectando con Gemini para generar transformaciones...")

                gemini_api_key = os.getenv("GEMINI_API_KEY")
                if not gemini_api_key:
                    st.error("Error: La variable de entorno GEMINI_API_KEY no está configurada. Por favor, añádela a tu archivo .env")
                else:
                    genai.configure(api_key=gemini_api_key)
                    model = genai.GenerativeModel('gemini-2.5-flash')
                    # --- Contexto Pentaho/ETL para Gemini ---
                    if tabla_schema_destino:
                        campos_destino = [
                            {
                                "campo_salida": campo.name,
                                "tipo": campo.tipo,
                                "obligatorio": campo.requerido,
                                **campo.metadata
                            }
                            for campo in tabla_schema_destino.fields
                        ]
                        metadatos = tabla_schema_destino.metadata if hasattr(tabla_schema_destino, 'metadata') else {}
                        contexto_gemini = {
                            "campos_salida": campos_destino,
                            "primary_key": getattr(tabla_schema_destino, 'primary_key', []),
                            "metadatos": metadatos
                        }
                    else:
                        contexto_gemini = {}
                    prompt = f"""
Eres un asistente experto en ETL. Dado el siguiente contexto de esquema de salida (campos de salida, tipos, obligatorios, metadatos, claves primarias):
{contexto_gemini}
Y los campos de entrada del CSV:
{headers_csv}
Sugiere transformaciones para convertir los datos del CSV al esquema de salida. Usa la siguiente estructura para cada transformación:
{{
  "campo_salida": "nombre del campo de salida",
  "campo_entrada": "nombre del campo de entrada" (si aplica),
  "tipo_transformacion": "mapping" | "calculo",
  "formula": "expresión o fórmula SQL si es cálculo",
  "descripcion": "explicación breve"
}}
Devuelve una lista JSON de transformaciones, usando SIEMPRE los nombres de clave anteriores.
"""
                    st.session_state['prompt_gemini'] = prompt
                    try:
                        response = model.generate_content(prompt)
                        st.info("Respuesta de Gemini recibida.")
                        st.text(f"DEBUG: Raw Gemini response: {response.text}")
                    except Exception as e:
                        st.error(f"Error al llamar a la API de Gemini: {e}")
                        return
                    try:
                        transformaciones_str = response.text.strip()
                        match = re.search(r"```json\s*(.*?)\s*```", transformaciones_str, re.DOTALL)
                        if not match:
                            match = re.search(r"```\s*(.*?)\s*```", transformaciones_str, re.DOTALL)
                        if match:
                            json_str = match.group(1)
                        else:
                            match = re.search(r"(\[.*?\])", transformaciones_str, re.DOTALL)
                            if match:
                                json_str = match.group(1)
                            else:
                                st.error("No se encontró un bloque JSON en la respuesta de Gemini. Revisa el prompt o la respuesta.")
                                raise ValueError("No se encontró un bloque JSON en la respuesta de Gemini.")
                        transformaciones_sugeridas = json.loads(json_str)
                        st.session_state['transformaciones_sugeridas'] = transformaciones_sugeridas
                        st.session_state['prompt_gemini'] = prompt
                        st.subheader("Transformaciones Sugeridas (Pentaho/ETL):")
                        if transformaciones_sugeridas:
                            st.dataframe(pd.DataFrame(transformaciones_sugeridas))
                        else:
                            st.info("No se han generado transformaciones sugeridas aún. Si esperabas verlas, revisa el prompt y la respuesta de Gemini.")
                    except json.JSONDecodeError as e:
                        st.error(f"Error al parsear la respuesta de Gemini como JSON: {e}")
                        st.text(response.text)
                    except Exception as e:
                        st.error(f"Error inesperado al procesar la respuesta de Gemini: {e}")
                        st.text(response.text)

        # Botón de aceptación SIEMPRE visible si hay sugeridas
        if st.session_state.get('transformaciones_sugeridas'):
            if st.button("Guardar Transformaciones (Pentaho/ETL)", key="aceptar_transformaciones"):
                st.session_state['transformaciones_aceptadas'] = st.session_state['transformaciones_sugeridas']
                st.success("¡Transformaciones guardadas! Ahora puedes probarlas o aplicarlas a tus datos reales.")

    # --- 2. Prueba con datos de prueba automática ---
    if st.session_state.get('transformaciones_aceptadas'):
        st.header("2. Prueba con datos de prueba automática")
        archivo_muestra_prueba = st.file_uploader("Sube un archivo CSV para probar las transformaciones (muestra)", type=["csv"], key="muestra_prueba")
        n_muestra = st.number_input("Tamaño de la muestra aleatoria", min_value=1, max_value=100, value=5, step=1, key="tamano_muestra")
        if archivo_muestra_prueba:
            df_prueba = pd.read_csv(archivo_muestra_prueba)
            muestra = df_prueba.sample(n=min(n_muestra, len(df_prueba)), random_state=42)
            st.write("Muestra aleatoria del CSV de entrada:")
            st.dataframe(muestra)
            try:
                df_muestra_transformada = muestra.copy()
                transformaciones_a_aplicar = st.session_state['transformaciones_aceptadas']
                st.write("Aplicando transformaciones ETL sobre la muestra...")
                for t in transformaciones_a_aplicar:
                    tipo = t.get("tipo_transformacion")
                    entrada = t.get("campo_entrada")
                    salida = t.get("campo_salida")
                    formula = t.get("formula")
                    if tipo == "mapping" and entrada and salida:
                        if entrada in df_muestra_transformada.columns:
                            df_muestra_transformada[salida] = df_muestra_transformada[entrada]
                    elif tipo == "calculo" and salida and formula:
                        # Aquí podrías implementar lógica para evaluar fórmulas simples
                        df_muestra_transformada[salida] = None
                # --- Solo columnas de salida en la muestra transformada ---
                columnas_salida = [t['campo_salida'] for t in transformaciones_a_aplicar if 'campo_salida' in t]
                for col in columnas_salida:
                    if col not in df_muestra_transformada.columns:
                        df_muestra_transformada[col] = None
                df_muestra_transformada = df_muestra_transformada[columnas_salida]
                st.write("Resultado de la muestra transformada (solo columnas de salida):")
                st.dataframe(df_muestra_transformada)
                st.success("Transformaciones aplicadas automáticamente sobre la muestra. Si el resultado es correcto, puedes proceder a transformar todo el dataset.")
            except Exception as e:
                st.error(f"Error al aplicar las transformaciones sobre la muestra: {e}")

    # --- 3. Input para datos reales a convertir ---
    if st.session_state.get('transformaciones_aceptadas'):
        st.header("3. Input para datos reales a convertir")
        archivo_datos_transformar = st.file_uploader("Sube el archivo de datos para transformar (CSV)", type=["csv"], key="data_transformar")
        if archivo_datos_transformar:
            if st.button("Aplicar Transformaciones (ETL)"):
                df_original = pd.read_csv(archivo_datos_transformar)
                df_transformado = df_original.copy()
                transformaciones_a_aplicar = st.session_state['transformaciones_aceptadas']
                for t in transformaciones_a_aplicar:
                    tipo = t.get("tipo_transformacion")
                    entrada = t.get("campo_entrada")
                    salida = t.get("campo_salida")
                    formula = t.get("formula")
                    if tipo == "mapping" and entrada and salida:
                        if entrada in df_transformado.columns:
                            df_transformado[salida] = df_transformado[entrada]
                    elif tipo == "calculo" and salida and formula:
                        df_transformado[salida] = None # Placeholder para cálculos
                # --- Solo columnas de salida en el resultado final ---
                columnas_salida = [t['campo_salida'] for t in transformaciones_a_aplicar if 'campo_salida' in t]
                for col in columnas_salida:
                    if col not in df_transformado.columns:
                        df_transformado[col] = None
                df_transformado = df_transformado[columnas_salida]
                st.subheader("Datos Transformados (Pentaho/ETL, solo columnas de salida):")
                st.dataframe(df_transformado)

                # --- Resumen de campos obligatorios vacíos ---
                try:
                    tabla_schema_destino = load_schema(str(esquema_destino_comparacion))
                    campos_obligatorios = [f.name for f in tabla_schema_destino.fields if getattr(f, 'requerido', False)]
                    vacios = {col: df_transformado[col].isnull().sum() + (df_transformado[col] == '').sum() for col in campos_obligatorios if col in df_transformado.columns}
                    vacios = {k: v for k, v in vacios.items() if v > 0}
                    if vacios:
                        st.warning(f"Campos obligatorios vacíos/nulos tras la transformación: {vacios}")
                    else:
                        st.info("Todos los campos obligatorios tienen valor en el resultado transformado.")
                except Exception as e:
                    st.info(f"No se pudo calcular el resumen de campos obligatorios vacíos: {e}")

                # --- Validación semántica y SHACL por detrás ---
                try:
                    global_tbox = build_global_tbox()
                    tabla_schema_destino = load_schema(str(esquema_destino_comparacion))
                    builder = OntologyBuilder(global_tbox)
                    grafo_owl = builder.build_abox_graph(tabla_schema_destino, df_transformado)
                    with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False) as tmp:
                        grafo_owl.serialize(destination=tmp.name, format="turtle")
                        owl_path = tmp.name
                    st.info(f"OWL (TBox + ABox) generado para validación: {owl_path}")
                    razonado, logs_hermit = _run_hermit_sync(owl_path)
                    st.markdown("**Validación semántica (HermiT):** " + ("✅ Consistente" if razonado else "❌ Inconsistente"))
                    with st.expander("Log HermiT", expanded=False):
                        st.text(logs_hermit)
                    shape_ttl = generar_shape_shacl(tabla_schema_destino)
                    with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False, mode="w") as tmp_sh:
                        tmp_sh.write(shape_ttl)
                        shacl_path = tmp_sh.name
                    valido, logs_shacl = validate_shacl(owl_path, shacl_path)
                    st.markdown("**Validación SHACL:** " + ("✅ Válido" if valido else "❌ Inválido"))
                    with st.expander("Log SHACL", expanded=False):
                        st.text(logs_shacl)
                except Exception as e:
                    st.warning(f"No se pudo validar semánticamente el resultado: {e}")

                csv_output = df_transformado.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Descargar Datos Transformados (CSV)",
                    data=csv_output,
                    file_name="datos_transformados.csv",
                    mime="text/csv",
                )
                st.success("¡Transformaciones aplicadas con éxito!")

if __name__ == "__main__":
    main()
