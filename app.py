import streamlit as st
import pandas as pd
import yaml
from pathlib import Path
from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal, URIRef
import subprocess
import os
import tempfile

# --- Configuración ---
SCHEMA_DIR = Path("schema_yaml")
BASE = Namespace("http://bocagrande.local/ont#")
HERMIT_JAR = str(Path("HermiT/HermiT.jar"))

st.set_page_config(page_title="Conversor Semántico OWL + HermiT")
st.title("🦉 Conversor Semántico Universal (OWL + HermiT)")

# --- Paso 1: Leer todos los YAML y construir la TBox ---
def construir_owl_desde_yaml(yaml_dir, datos_df=None, clase_objetivo=None):
    g = Graph()
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("bg", BASE)
    clases = {}
    props = {}
    # 1. Clases y propiedades
    for yfile in yaml_dir.glob("*.yaml"):
        with open(yfile, "r") as f:
            try:
                schema = yaml.safe_load(f)
            except Exception as e:
                continue
        class_name = yfile.stem.upper()
        class_uri = BASE[class_name]
        g.add((class_uri, RDF.type, OWL.Class))
        clases[class_name] = class_uri
        # Propiedades
        # Handle the actual YAML structure where fields are in a list with "Campo" keys
        if isinstance(schema, list):
            # Extract field names from the list structure
            for field in schema:
                if isinstance(field, dict) and "Campo" in field:
                    colname = field["Campo"]
                    prop_uri = BASE[str(colname).upper()]
                    g.add((prop_uri, RDF.type, OWL.DatatypeProperty))
                    g.add((prop_uri, RDFS.domain, class_uri))
                    props[colname] = prop_uri
        else:
            # Fallback for the old structure
            cols = schema.get("columns") or schema.get("campos") or []
            for col in cols:
                if isinstance(col, dict) and "name" in col:
                    colname = col["name"]
                elif isinstance(col, str):
                    colname = col
                else:
                    continue
                prop_uri = BASE[str(colname).upper()]
                g.add((prop_uri, RDF.type, OWL.DatatypeProperty))
                g.add((prop_uri, RDFS.domain, class_uri))
                props[colname] = prop_uri
    # 2. Instancias (ABox)
    if datos_df is not None and clase_objetivo is not None:
        for idx, row in datos_df.iterrows():
            ind_uri = BASE[f"{clase_objetivo}_{idx+1}"]
            g.add((ind_uri, RDF.type, clases[clase_objetivo]))
            for col in row.index:
                if col in props:
                    g.add((ind_uri, props[col], Literal(row[col])))
    return g

# --- Paso 2: UI para subir datos y elegir clase ---
st.markdown("Sube un archivo de datos (CSV) y elige a qué clase YAML corresponde para generar individuos OWL. El sistema generará la ontología completa y validará con HermiT.")

uploaded_file = st.file_uploader("Archivo de datos (CSV)", type=["csv"])

# Detectar clases disponibles
yaml_files = list(SCHEMA_DIR.glob("*.yaml"))
clase_opciones = [y.stem.upper() for y in yaml_files]
clase_seleccionada = st.selectbox("Clase objetivo para instancias", clase_opciones)

if st.button("Generar OWL y validar con HermiT"):
    logs = ""
    if uploaded_file is None:
        st.error("Debes subir un archivo de datos.")
    else:
        try:
            df = pd.read_csv(uploaded_file)
            logs += f"Datos cargados: {df.shape[0]} filas, {df.shape[1]} columnas\n"
            g = construir_owl_desde_yaml(SCHEMA_DIR, df, clase_seleccionada)
            with tempfile.NamedTemporaryFile(delete=False, suffix=".owl") as tmp_owl:
                owl_path = tmp_owl.name
                g.serialize(owl_path, format="xml")
            logs += f"OWL serializado en: {owl_path}\n"
            # Ejecutar HermiT
            cmd = ["java", "-jar", HERMIT_JAR, "-input", owl_path]
            logs += f"Ejecutando HermiT: {' '.join(cmd)}\n"
            result = subprocess.run(cmd, capture_output=True, text=True)
            logs += result.stdout
            logs += result.stderr
            st.success("Proceso completado. Ver logs y descarga de OWL.")
            st.download_button("Descargar OWL generado", data=open(owl_path, "rb").read(), file_name="output.owl")
        except Exception as e:
            logs += f"Error: {e}\n"
        st.text_area("Logs del proceso", logs, height=400) 