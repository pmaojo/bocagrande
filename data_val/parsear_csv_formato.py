import csv
import re
import yaml
import argparse
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def parsear_csv_formato(ruta_csv):
    tablas = {}
    tabla_actual = None
    cabecera_actual = None
    num_columnas = 0
    patron_titulo = re.compile(r"^\d+\.\s+.*\(T_(.*)\.csv\)", re.IGNORECASE)

    with open(ruta_csv, "r", encoding="utf-8", newline='') as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        for row in reader:
            row = [c.strip() for c in row]
            if not any(row):
                continue
            m = patron_titulo.match(row[0])
            if m:
                tabla_actual = m.group(1).upper()
                tablas[tabla_actual] = []
                cabecera_actual = None
                num_columnas = 0
                logging.info(f"Detectada tabla: {tabla_actual}")
                continue
            if row[0].lower() == "campo":
                cabecera_actual = row
                num_columnas = len(row)
                tablas[tabla_actual].append({"__cabecera__": cabecera_actual})
                continue
            if not tabla_actual or not cabecera_actual:
                continue
            if row[0].lower() == "campo":
                cabecera_actual = row
                num_columnas = len(row)
                tablas[tabla_actual].append({"__cabecera__": cabecera_actual})
                continue
            if len(row) < num_columnas:
                row += [""] * (num_columnas - len(row))
            if len(row) > num_columnas:
                row = row[:num_columnas-1] + [",".join(row[num_columnas-1:])]
            campo = dict(zip(cabecera_actual, row))
            tablas[tabla_actual].append(campo)
    return tablas

def normalizar_tipo(tipo):
    tipo_map = {
        "Alfanumérico": "string",
        "Numérico": "integer",
        "Decimal": "float",
        "Fecha": "date",
        "Date": "date",
        "Booleano": "boolean",
    }
    return tipo_map.get(tipo.strip(), tipo.strip())

def enriquecer_campo(campo):
    desc = campo.get("Descripción", "")
    formato = campo.get("Formato", "")
    enriquecido = dict(campo)
    # Enum: SI/NO, valores posibles, M= Mujer, H= Hombre, etc.
    enum = None
    # SI/NO
    if re.search(r"\bSI/?NO\b", desc, re.IGNORECASE):
        enum = ["SI", "NO"]
    # M= Mujer, H= Hombre
    elif re.search(r"M= ?Mujer", desc) and re.search(r"H= ?Hombre", desc):
        enum = ["M", "H"]
    # Valores posibles: [ ... ]
    m_enum = re.search(r"Valores posibles: *\[([^\]]+)\]", desc)
    if m_enum:
        valores = [v.strip() for v in m_enum.group(1).split(",")]
        enum = valores
    if enum:
        enriquecido["Enum"] = enum
    # Deprecado
    if "deprecado" in desc.lower():
        enriquecido["Deprecado"] = True
    # Formato de fecha
    if "fecha" in desc.lower() and formato:
        enriquecido["Formato"] = formato
    # Foreign key: patrón Tabla.campo
    m_fk = re.search(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", desc)
    if m_fk:
        enriquecido["foreign_key"] = f"{m_fk.group(1)}.{m_fk.group(2)}"
    return enriquecido

def exportar_tablas_a_yaml(tablas, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    for nombre_tabla, campos in tablas.items():
        campos_limpios = [c for c in campos if "__cabecera__" not in c]
        if not campos_limpios:
            continue
        # Normalizar tipos y enriquecer campos
        enriquecidos = []
        for campo in campos_limpios:
            if "Tipo" in campo:
                campo["Tipo"] = normalizar_tipo(campo["Tipo"])
            enriquecidos.append(enriquecer_campo(campo))
        ruta_yaml = Path(output_dir) / f"{nombre_tabla}.yaml"
        with open(ruta_yaml, "w", encoding="utf-8") as f:
            yaml.dump(enriquecidos, f, allow_unicode=True, sort_keys=False)
        logging.info(f"Exportado: {ruta_yaml}")

def extraer_metadatos_logicos(ruta_csv):
    import collections
    patron_titulo = re.compile(r"^(\d+)\.\s+.*\(T_(.*)\.csv\)", re.IGNORECASE)
    metadatos = collections.OrderedDict()
    tabla_actual = None
    buffer_notas = []
    campos_conocidos = set()
    with open(ruta_csv, "r", encoding="utf-8") as f:
        for linea in f:
            l = linea.strip().strip(',')
            if not l:
                continue
            m = patron_titulo.match(l)
            if m:
                tabla_actual = m.group(2).upper()
                if tabla_actual not in metadatos:
                    metadatos[tabla_actual] = {}
                buffer_notas = []
                campos_conocidos = set()
                continue
            # Detectar cabecera de campos para filtrar nombres de campo
            if l.lower().startswith("campo, ") or l.lower().startswith("campo,"):
                partes = [x.strip() for x in l.split(",") if x.strip()]
                campos_conocidos.update(partes)
                continue
            # Si es una línea de comentario o nota (no cabecera ni campo)
            if tabla_actual and not l.lower().startswith("campo") and not l.split(',')[0].strip() in ("", "Campo"):
                partes = [x.strip() for x in l.split(",")]
                # Heurística: solo añadir a notas si NO es una fila de campo (muchas comas y parece una definición de campo)
                es_fila_campo = (
                    len(partes) >= 4 and
                    all(partes[i] for i in range(3)) and
                    not any(kw in l.lower() for kw in ["clave", "unique", "deprecado", "restricción", "formato", "nota", "observación"])
                )
                if es_fila_campo:
                    continue  # Es una fila de campo, no nota
                # Heurística: buscar claves compuestas tipo 'La clave está compuesta por ...'
                l_lower = l.lower()
                if ("clave" in l_lower and "compuesta por" in l_lower) or ("clave" in l_lower and "combinación de" in l_lower):
                    campos = []
                    expr = None
                    if "compuesta por" in l_lower:
                        expr = l.split("compuesta por",1)[1].strip()
                    elif "combinación de" in l_lower:
                        expr = l.split("combinación de",1)[1].strip()
                    if expr:
                        if "+" in expr:
                            campos = [c.strip() for c in expr.split("+")]
                            op = "concat"
                        elif "," in expr:
                            campos = [c.strip() for c in expr.split(",")]
                            op = "concat"
                        elif " y " in expr:
                            campos = [c.strip() for c in expr.split(" y ")]
                            op = "concat"
                        else:
                            campos = [expr.strip()]
                            op = "field"
                        metadatos[tabla_actual].setdefault("formulas", {})["primary_key"] = {
                            "operation": op,
                            "fields": campos,
                            "expression": expr
                        }
                        metadatos[tabla_actual]["primary_key"] = campos
                if "unique" in l_lower:
                    campos = []
                    if ":" in l:
                        parte = l.split(":",1)[1]
                        campos = [c.strip().replace(' y ', ',').replace(' + ', ',').replace('.', '') for c in parte.split(',')]
                        campos = [c for c in ','.join(campos).split(',') if c]
                    else:
                        parte = l.split("deben ser UNIQUE")[0] if "deben ser UNIQUE" in l else l
                        campos = [c.strip() for c in parte.split(',') if c.strip()]
                    if campos:
                        metadatos[tabla_actual]["unique"] = campos
                # Acumular solo notas/comentarios
                buffer_notas.append(l)
                metadatos[tabla_actual]["notes"] = buffer_notas.copy()
    return metadatos

def exportar_metadatos_logicos(metadatos, output_path):
    # Convierte OrderedDict a dict normal para evitar marcas de PyYAML
    metadatos_puro = {k: dict(v) for k, v in metadatos.items()}
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(metadatos_puro, f, allow_unicode=True, sort_keys=False)
    logging.info(f"Exportado YAML de metadatos lógicos: {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Parsea formatoSalida.csv y exporta cada tabla principal como YAML.")
    parser.add_argument("--csv", required=True, help="Ruta al archivo formatoSalida.csv")
    parser.add_argument("--out", required=True, help="Directorio de salida para los YAMLs")
    parser.add_argument("--meta-yaml", default="metadatos_tablas.yaml", help="Ruta para exportar el YAML de metadatos lógicos")
    args = parser.parse_args()

    tablas = parsear_csv_formato(args.csv)
    exportar_tablas_a_yaml(tablas, args.out)

    metadatos = extraer_metadatos_logicos(args.csv)
    exportar_metadatos_logicos(metadatos, args.meta_yaml)

if __name__ == "__main__":
    main() 