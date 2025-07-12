import csv
from collections import defaultdict
from typing import Dict, List, Any, Tuple
import re
import os

# --- Heurística robusta para parsear el CSV de especificación ---
def es_inicio_tabla(row: List[str]) -> bool:
    return row[0].strip().startswith(tuple(f"{i}." for i in range(1, 30)))

def es_header_campos(row: List[str]) -> bool:
    return row[0].strip().lower() == "campo"

def es_fila_vacia(row: List[str]) -> bool:
    return all(not c.strip() for c in row)

def normaliza_nombre_tabla(raw: str) -> str:
    # Extrae el nombre lógico de la tabla del título
    if "(" in raw and ")" in raw:
        nombre = raw.split("(")[-1].split(")")[0]
        nombre = nombre.replace("T_", "").replace(".csv", "").replace(".CSV", "").strip().upper()
        return nombre
    return raw.strip().upper()

def es_fila_campo(row: List[str]) -> bool:
    tipos_validos = {"Numérico", "Alfanumérico", "Fecha", "Decimal"}
    return (
        len(row) >= 3 and
        row[0] and row[1] and
        row[1].strip().capitalize() in tipos_validos and
        not any(palabra in row[0].lower() for palabra in ["la clave", "campo deprecado", "nota", "requerimientos", "único", "unique"])
    )

def enriquecer_metadatos(metadatos: dict):
    for tabla, meta in metadatos.items():
        notes = meta.get('notes', [])
        nuevas_notes = []
        for note in notes:
            # Clave primaria por combinación
            m = re.search(r'clave.*combinaci[oó]n de ([a-zA-Z0-9_ +]+)', note, re.IGNORECASE)
            if m:
                campos = [c.strip() for c in re.split(r'\\+|\+|y', m.group(1)) if c.strip()]
                meta.setdefault('formulas', {})['primary_key'] = {
                    'operation': 'concat',
                    'fields': campos,
                    'expression': ' + '.join(campos)
                }
                meta['primary_key'] = campos
                continue  # No añadas la nota como texto libre
            # Clave compuesta por
            m = re.search(r'clave.*compuesta por ([a-zA-Z0-9_ +]+)', note, re.IGNORECASE)
            if m:
                campos = [c.strip() for c in re.split(r'\\+|\+|y', m.group(1)) if c.strip()]
                meta.setdefault('formulas', {})['primary_key'] = {
                    'operation': 'concat',
                    'fields': campos,
                    'expression': ' + '.join(campos)
                }
                meta['primary_key'] = campos
                continue
            # UNIQUE
            m = re.search(r'deben ser UNIQUE.*?([a-zA-Z0-9_, ]+)', note, re.IGNORECASE)
            if m:
                campos = [c.strip() for c in m.group(1).split(',') if c.strip()]
                meta['unique'] = campos
                continue
            # Clave de unicidad
            m = re.search(r'clave de unicidad.*relaci[oó]n entre ([a-zA-Z0-9_ +]+)', note, re.IGNORECASE)
            if m:
                campos = [c.strip() for c in re.split(r'\\+|\+|y', m.group(1)) if c.strip()]
                meta.setdefault('formulas', {})['unique_key'] = {
                    'operation': 'concat',
                    'fields': campos,
                    'expression': ' + '.join(campos)
                }
                meta['unique'] = campos
                continue
            # Si no es ninguna de las anteriores, deja la nota
            nuevas_notes.append(note)
        if nuevas_notes:
            meta['notes'] = nuevas_notes
        else:
            meta.pop('notes', None)

def parsear_csv_especificacion(path: str) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Any]]:
    tablas = {}
    metadatos = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        estado = "fuera"
        nombre_tabla = None
        header = []
        campos = []
        notas = []
        for row in reader:
            if not row or es_fila_vacia(row):
                if estado == "en_tabla" and nombre_tabla and nombre_tabla.strip():
                    tablas[nombre_tabla] = [c for c in campos if c.get(header[0]) and c.get(header[0]).strip()]
                    if notas:
                        metadatos.setdefault(nombre_tabla, {})["notes"] = notas
                    campos, notas = [], []
                    estado = "fuera"
                continue
            if es_inicio_tabla(row):
                if estado == "en_tabla" and nombre_tabla and nombre_tabla.strip():
                    tablas[nombre_tabla] = [c for c in campos if c.get(header[0]) and c.get(header[0]).strip()]
                    if notas:
                        metadatos.setdefault(nombre_tabla, {})["notes"] = notas
                    campos, notas = [], []
                nombre_tabla = normaliza_nombre_tabla(row[0])
                if not nombre_tabla or not nombre_tabla.strip():
                    nombre_tabla = None
                    estado = "fuera"
                    continue
                estado = "comentarios"
                continue
            if estado == "comentarios":
                if es_header_campos(row):
                    header = row
                    estado = "en_tabla"
                else:
                    # Solo añade a notas si NO es fila de campo
                    if row[0].strip() and not es_fila_campo(row):
                        notas.append(row[0].strip())
                continue
            if estado == "en_tabla":
                if es_header_campos(row):
                    # Repetición de header, ignora
                    continue
                # Considera campo solo si la primera columna no está vacía y no es 'Campo'
                if len(row) >= 2 and row[0].strip() and row[0].strip().lower() != "campo":
                    if not row[0] or not row[0].strip():
                        continue
                    # Rellena con None si faltan columnas
                    fila = list(row) + [None] * (len(header) - len(row))
                    campos.append(dict(zip(header, fila)))
                else:
                    # Solo añade a notas si NO es fila de campo
                    if row[0].strip() and not es_fila_campo(row):
                        notas.append(row[0].strip())
        # Última tabla
        if estado == "en_tabla" and nombre_tabla and nombre_tabla.strip():
            tablas[nombre_tabla] = [c for c in campos if c.get(header[0]) and c.get(header[0]).strip()]
            if notas:
                metadatos.setdefault(nombre_tabla, {})["notes"] = notas
    enriquecer_metadatos(metadatos)
    return tablas, metadatos

def exportar_metadatos_por_tabla(metadatos: dict, output_dir: str):
    import yaml
    os.makedirs(output_dir, exist_ok=True)
    for tabla, meta in metadatos.items():
        path = os.path.join(output_dir, f"{tabla}.metadatos.yaml")
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump({tabla: meta}, f, allow_unicode=True, sort_keys=False)

# --- Ejemplo de uso ---
if __name__ == "__main__":
    tablas, metadatos = parsear_csv_especificacion("data/input/formatoSalida.csv")
    # Exporta todos los metadatos en un solo archivo
    import yaml
    with open("data/output/metadatos.yaml", "w", encoding="utf-8") as f:
        yaml.dump(metadatos, f, allow_unicode=True, sort_keys=False)
    # Exporta los metadatos limpios por tabla a data/output_yaml/
    exportar_metadatos_por_tabla(metadatos, "data/output_yaml/") 