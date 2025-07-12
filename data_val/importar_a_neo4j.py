from neo4j import GraphDatabase
import yaml
import os

NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "etletletl"

def cargar_yamls(directorio):
    yamls = {}
    for archivo in os.listdir(directorio):
        if archivo.endswith('.yaml') and archivo != 'metadatos_tablas.yaml':
            nombre_tabla = archivo.replace('.yaml', '').upper()
            with open(os.path.join(directorio, archivo), 'r', encoding='utf-8') as f:
                yamls[nombre_tabla] = yaml.safe_load(f)
    return yamls

def cargar_metadatos_tablas(ruta):
    if not os.path.exists(ruta):
        return {}
    with open(ruta, 'r', encoding='utf-8') as f:
        meta = yaml.safe_load(f)
    # Normaliza claves a mayúsculas
    return {k.upper(): v for k, v in meta.items()}

def importar_a_neo4j(yamls, metadatos):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        # Crea nodos de tabla con metadatos
        for tabla, campos in yamls.items():
            props = {"nombre": tabla}
            meta = metadatos.get(tabla, {})
            # Añade metadatos principales como propiedades
            for k in ["primary_key", "unique", "formulas", "notes"]:
                if k in meta:
                    props[k] = str(meta[k])
            session.run(
                "MERGE (t:Tabla {nombre: $nombre}) SET t += $props",
                nombre=tabla, props=props
            )
            for c in campos:
                # Crea nodos de campo
                session.run(
                    """
                    MERGE (c:Campo {nombre: $nombre, tabla: $tabla})
                    SET c.tipo = $tipo, c.longitud = $longitud, c.enum = $enum, c.deprecado = $deprecado, c.formato = $formato, c.foreign_key = $foreign_key
                    """,
                    nombre=c.get('Campo',''),
                    tabla=tabla,
                    tipo=c.get('Tipo',''),
                    longitud=c.get('Longitud',''),
                    enum='|'.join(c['Enum']) if 'Enum' in c else '',
                    deprecado=bool(c.get('Deprecado', False)),
                    formato=c.get('Formato',''),
                    foreign_key=c.get('foreign_key','')
                )
                # Relación HAS_FIELD
                session.run(
                    """
                    MATCH (t:Tabla {nombre: $tabla}), (c:Campo {nombre: $nombre, tabla: $tabla})
                    MERGE (t)-[:HAS_FIELD]->(c)
                    """,
                    tabla=tabla, nombre=c.get('Campo','')
                )
                # Relación FOREIGN_KEY (normaliza tabla referenciada a mayúsculas)
                if c.get('foreign_key'):
                    ref_tabla = c['foreign_key'].split('.')[0].upper()
                    session.run(
                        """
                        MATCH (c:Campo {nombre: $nombre, tabla: $tabla}), (t2:Tabla {nombre: $ref_tabla})
                        MERGE (c)-[:FOREIGN_KEY]->(t2)
                        """,
                        nombre=c.get('Campo',''), tabla=tabla, ref_tabla=ref_tabla
                    )
    driver.close()

if __name__ == '__main__':
    yamls = cargar_yamls('data/output_yaml')
    metadatos = cargar_metadatos_tablas('data/output_yaml/metadatos_tablas.yaml')
    importar_a_neo4j(yamls, metadatos)
    print('¡Importación a Neo4j completada!') 