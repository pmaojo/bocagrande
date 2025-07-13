"""
Interfaz: UI Streamlit para orquestar el pipeline semántico.
"""
import tempfile
import streamlit as st
import sys
import os
import pandas as pd # Añadir import para pandas
import numpy as np
import traceback
from bocagrande.langchain_agent import generate_steps
import re
import io
from pathlib import Path
import pandas.errors

# Añadir la raíz del proyecto al sys.path para asegurar la detección de módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from adapter.yaml_loader import load_schema
from adapter.csv_loader import read_csv
from adapter.hermit_runner import HermiTReasoner
from ontology.tbox_builder import build_global_tbox
from bocagrande.transform import ETLStep, apply_transformations
from bocagrande.validation import validate_dataframe

# Estructura básica de la UI


# Cargar la TBox global al inicio de la aplicación (se cachea con @st.cache_resource)
@st.cache_resource
def get_global_tbox():
    return build_global_tbox()

# --- Razonería HermiT reutilizable ---
@st.cache_resource
def get_reasoner() -> HermiTReasoner:
    """Return a cached HermiT reasoner instance."""
    return HermiTReasoner()

# --- Utilidad universal para leer archivos a DataFrame ---
def leer_a_dataframe(archivo, sin_cabecera=False):
    nombre = archivo.name.lower()
    try:
        if nombre.endswith('.csv'):
            if sin_cabecera:
                return pd.read_csv(archivo, header=None)
            else:
                return pd.read_csv(archivo)
        elif nombre.endswith('.xlsx') or nombre.endswith('.xls'):
            return pd.read_excel(archivo, header=None if sin_cabecera else 0)
        elif nombre.endswith('.parquet'):
            return pd.read_parquet(archivo)
        else:
            st.error(f"Formato de archivo no soportado: {nombre}")
            return None
    except pandas.errors.EmptyDataError:
        st.error("El archivo está vacío o no tiene columnas para parsear. Por favor, revisa el archivo de entrada.")
        return None
    except Exception as e:
        st.error(f"Error al leer el archivo: {e}")
        return None

# --- Utilidad universal para exportar DataFrame ---
def exportar_dataframe(df, formato):
    buffer = io.BytesIO()
    if formato == 'csv':
        return df.to_csv(index=False).encode('utf-8')
    elif formato == 'excel':
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        return buffer.getvalue()
    elif formato == 'parquet':
        df.to_parquet(buffer, index=False)
        return buffer.getvalue()
    else:
        st.error(f"Formato de exportación no soportado: {formato}")
        return None

def main():
    """
    Orquesta la carga de archivos, comparación, prueba y aplicación de transformaciones.
    """
    st.title("Conversor Semántico Universal ")
    st.sidebar.info("Flujo: Comparación, prueba y transformación de datos.")

    reasoner = get_reasoner()

    # --- 1. Comparación y Generación de Transformaciones ---
    st.header("1. Comparación y Generación de Transformaciones")
    sin_cabecera = st.checkbox("El archivo no tiene cabecera (primera fila)", key="sin_cabecera")
    archivo_muestra_comparacion = st.file_uploader("Sube un archivo de muestra para comparación (CSV, Excel, Parquet)", type=["csv", "xlsx", "xls", "parquet"], key="comparacion_data")
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
        df_muestra = leer_a_dataframe(archivo_muestra_comparacion, sin_cabecera)
        if df_muestra is None:
            st.error("El archivo está vacío, corrupto o no tiene columnas para parsear. Por favor, revisa el archivo de entrada.")
            st.stop()
        st.write("**Campos de entrada detectados:**", list(df_muestra.columns))
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
            df_muestra = leer_a_dataframe(archivo_muestra_comparacion)
            if df_muestra is None:
                st.error("El archivo está vacío, corrupto o no tiene columnas para parsear. Por favor, revisa el archivo de entrada.")
                st.stop()
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
                    from langchain_google_genai import ChatGoogleGenerativeAI
                    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", google_api_key=gemini_api_key)
                    try:
                        steps = generate_steps(headers_csv, tabla_schema_destino, llm)
                        st.session_state['transformaciones_sugeridas'] = [s.__dict__ for s in steps]
                        st.dataframe(pd.DataFrame(st.session_state['transformaciones_sugeridas']))
                    except Exception as e:
                        st.error(f"Error al generar transformaciones: {e}")

        # Botón de aceptación SIEMPRE visible si hay sugeridas
        if st.session_state.get('transformaciones_sugeridas'):
            if st.button("Guardar Transformaciones (ETL)", key="aceptar_transformaciones"):
                st.session_state['transformaciones_aceptadas'] = st.session_state['transformaciones_sugeridas']
                st.success("¡Transformaciones guardadas! Ahora puedes probarlas o aplicarlas a tus datos reales.")

    # --- 2. Prueba con datos de prueba automática ---
    if st.session_state.get('transformaciones_aceptadas'):
        st.header("2. Prueba con datos de prueba automática")
        archivo_muestra_prueba = st.file_uploader("Sube un archivo CSV para probar las transformaciones (muestra)", type=["csv"], key="muestra_prueba")
        n_muestra = st.number_input("Tamaño de la muestra aleatoria", min_value=1, max_value=100, value=5, step=1, key="tamano_muestra")
        if archivo_muestra_prueba:
            df_prueba = leer_a_dataframe(archivo_muestra_prueba)
            muestra = df_prueba.sample(n=min(n_muestra, len(df_prueba)), random_state=42)
            st.write("Muestra aleatoria del CSV de entrada:")
            st.dataframe(muestra)
            try:
                steps = [ETLStep(**t) for t in st.session_state['transformaciones_aceptadas']]
                st.write("Aplicando transformaciones ETL sobre la muestra...")
                df_muestra_transformada, _, _, _ = apply_transformations(muestra, steps)
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
                df_original = leer_a_dataframe(archivo_datos_transformar)
                steps = [ETLStep(**t) for t in st.session_state['transformaciones_aceptadas']]
                df_transformado, _, _, _ = apply_transformations(df_original, steps)
                st.subheader("Datos Transformados (ETL, solo columnas de salida):")
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
                    tabla_schema_destino = load_schema(str(esquema_destino_comparacion))
                    hermit_ok, shacl_ok, logs_hermit, logs_shacl = validate_dataframe(
                        df_transformado,
                        tabla_schema_destino,
                        reasoner=reasoner,
                    )
                    st.markdown(
                        "**Validación semántica (HermiT):** "
                        + ("✅ Consistente" if hermit_ok else "❌ Inconsistente")
                    )
                    with st.expander("Log HermiT", expanded=False):
                        st.text(logs_hermit)
                    st.markdown(
                        "**Validación SHACL:** " + ("✅ Válido" if shacl_ok else "❌ Inválido")
                    )
                    with st.expander("Log SHACL", expanded=False):
                        st.text(logs_shacl)
                except Exception as e:
                    st.warning(f"No se pudo validar semánticamente el resultado: {e}")

                # --- Ejemplo de lógica de transformación (simplificado) ---
                # (esto debe ir en el bloque donde aplicas las transformaciones)
                if archivo_datos_transformar and 'transformaciones_aceptadas' in st.session_state:
                    df = leer_a_dataframe(archivo_datos_transformar)
                    steps = [ETLStep(**t) for t in st.session_state['transformaciones_aceptadas']]
                    sobrescribir = st.session_state.get('sobrescribir', True)
                    df_result, generados, sobrescritos, faltantes = apply_transformations(
                        df,
                        steps,
                        overwrite=sobrescribir,
                    )
                    st.subheader("Datos Transformados (solo columnas de salida):")
                    st.dataframe(df_result)
                    st.info(f"Campos generados: {generados}")
                    st.info(f"Campos sobrescritos: {sobrescritos}")
                    if faltantes:
                        st.warning(f"Campos faltantes/no generados: {faltantes}")
                    formato_export = st.selectbox("Formato de exportación", ["csv", "excel", "parquet"])
                    datos_export = exportar_dataframe(df_result, formato_export)
                    st.download_button(
                        "Descargar datos transformados",
                        datos_export,
                        file_name=f"datos_transformados.{formato_export if formato_export != 'excel' else 'xlsx'}",
                    )

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