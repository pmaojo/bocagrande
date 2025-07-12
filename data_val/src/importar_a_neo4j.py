import os
import yaml
from neo4j import GraphDatabase

# Configuración Neo4j
NEO4J_URI = os.environ.get("NEO4J_URI", "neo4j://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "etletletl")

# Rutas
YAML_DIR = "data/output_yaml"

# Utilidades

def cargar_yaml(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_tablas_y_campos():
    tablas = {}
    for fname in os.listdir(YAML_DIR):
        if fname.endswith(".yaml") and not fname.endswith(".metadatos.yaml") and fname != "metadatos_tablas.yaml":
            tabla = fname.replace(".yaml", "")
            tablas[tabla] = cargar_yaml(os.path.join(YAML_DIR, fname))
    return tablas

def get_metadatos():
    metadatos = {}
    for fname in os.listdir(YAML_DIR):
        if fname.endswith(".metadatos.yaml"):
            tabla = fname.replace(".metadatos.yaml", "")
            metadatos[tabla] = cargar_yaml(os.path.join(YAML_DIR, fname)).get(tabla, {})
    return metadatos

def propiedades_planas(meta):
    # Solo deja propiedades planas (str, int, float, bool, list simple)
    planas = {}
    for k, v in meta.items():
        if isinstance(v, (str, int, float, bool)):
            planas[k] = v
        elif isinstance(v, list) and all(isinstance(x, (str, int, float, bool)) for x in v):
            planas[k] = v
    return planas

def importar_a_neo4j():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    tablas = get_tablas_y_campos()
    metadatos = get_metadatos()
    with driver.session() as session:
        # Limpia el grafo
        session.run("MATCH (n) DETACH DELETE n")
        # Crea nodos de tabla y campo
        for tabla, filas in tablas.items():
            if not tabla or not tabla.strip():
                continue
            props = propiedades_planas(metadatos.get(tabla, {}))
            session.run("MERGE (t:Tabla {nombre: $nombre}) SET t += $props", nombre=tabla, props=props)
            if filas:
                for campo in filas:
                    nombre_campo = campo.get("Campo") or campo.get("nombre")
                    if not nombre_campo or not nombre_campo.strip():
                        continue
                    session.run(
                        """
                        MERGE (c:Campo {nombre: $nombre_campo, tabla: $tabla})
                        SET c += $props
                        WITH c
                        MATCH (t:Tabla {nombre: $tabla})
                        MERGE (t)-[:HAS_FIELD]->(c)
                        """,
                        nombre_campo=nombre_campo,
                        tabla=tabla,
                        props=campo
                    )
        # Crea relaciones de claves primarias, únicas y foreign keys
        for tabla, meta in metadatos.items():
            if not tabla or not tabla.strip():
                continue
            # Clave primaria
            pk = meta.get("primary_key")
            if pk:
                for campo in pk:
                    if not campo or not str(campo).strip():
                        continue
                    session.run(
                        """
                        MATCH (t:Tabla {nombre: $tabla})-[:HAS_FIELD]->(c:Campo {nombre: $campo})
                        MERGE (t)-[:PRIMARY_KEY]->(c)
                        """,
                        tabla=tabla,
                        campo=campo
                    )
            # Claves únicas
            unique = meta.get("unique")
            if unique:
                for campo in unique:
                    if not campo or not str(campo).strip():
                        continue
                    session.run(
                        """
                        MATCH (t:Tabla {nombre: $tabla})-[:HAS_FIELD]->(c:Campo {nombre: $campo})
                        MERGE (t)-[:UNIQUE_KEY]->(c)
                        """,
                        tabla=tabla,
                        campo=campo
                    )
            # Notas y fórmulas como propiedades string YAML
            notes = meta.get("notes")
            if notes:
                notes_str = yaml.safe_dump(notes, allow_unicode=True, sort_keys=False)
                session.run(
                    "MATCH (t:Tabla {nombre: $tabla}) SET t.notes = $notes_str",
                    tabla=tabla,
                    notes_str=notes_str
                )
            formulas = meta.get("formulas")
            if formulas:
                formulas_str = yaml.safe_dump(formulas, allow_unicode=True, sort_keys=False)
                session.run(
                    "MATCH (t:Tabla {nombre: $tabla}) SET t.formulas = $formulas_str",
                    tabla=tabla,
                    formulas_str=formulas_str
                )
        # Foreign keys (heurística: busca campos con formato 'OTRATABLA.campo' en descripción)
        for tabla, filas in tablas.items():
            if not tabla or not tabla.strip():
                continue
            for campo in filas:
                nombre_campo = campo.get("Campo") or campo.get("nombre")
                if not nombre_campo or not nombre_campo.strip():
                    continue
                descripcion = campo.get("Descripción") or campo.get("descripcion")
                if descripcion and "." in descripcion:
                    partes = descripcion.split(".")
                    if len(partes) == 2:
                        ref_tabla, ref_campo = partes[0].strip().upper(), partes[1].strip()
                        if not ref_tabla or not ref_campo:
                            continue
                        session.run(
                            """
                            MATCH (t1:Tabla {nombre: $tabla})-[:HAS_FIELD]->(c1:Campo {nombre: $campo})
                            MATCH (t2:Tabla {nombre: $ref_tabla})-[:HAS_FIELD]->(c2:Campo {nombre: $ref_campo})
                            MERGE (c1)-[:FOREIGN_KEY]->(c2)
                            """,
                            tabla=tabla,
                            campo=nombre_campo,
                            ref_tabla=ref_tabla,
                            ref_campo=ref_campo
                        )
    driver.close()

if __name__ == "__main__":
    importar_a_neo4j() 