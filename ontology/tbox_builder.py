"""
Dominio: servicio para construir la TBox (ontología de clases y propiedades).
"""
from rdflib import Graph, RDF, RDFS, OWL, XSD
from pathlib import Path
from adapter.yaml_loader import load_schema

from .utils import BASE, limpiar_para_uri

def build_global_tbox(schema_dir: str = "schema_yaml") -> Graph:
    """
    Construye un grafo OWL (TBox) con todas las clases y propiedades definidas en los YAMLs.
    """
    g = Graph()
    g.bind("owl", OWL)
    g.bind("rdfs", RDFS)
    g.bind("bg", BASE)
    g.bind("xsd", XSD) # Bindear XSD para tipos de datos

    schema_path = Path(schema_dir)
    for yaml_file in schema_path.glob("*.yaml"):
        if "metadatos" in yaml_file.name.lower(): # Asegurarse de que ignore los metadatos
            continue 
        
        try:
            tabla_schema = load_schema(str(yaml_file))
            if tabla_schema is None: # Ignorar si load_schema devolvió None
                continue

            class_uri = BASE[limpiar_para_uri(tabla_schema.name.upper())]
            g.add((class_uri, RDF.type, OWL.Class))

            for campo in tabla_schema.fields:
                prop_uri = BASE[limpiar_para_uri(campo.name.upper())]
                g.add((prop_uri, RDF.type, OWL.DatatypeProperty))
                g.add((prop_uri, RDFS.domain, class_uri))
                # Añadir sh:range o xsd:datatype más robusto aquí
                if campo.tipo == "integer":
                    g.add((prop_uri, RDFS.range, XSD.integer))
                elif campo.tipo == "float":
                    g.add((prop_uri, RDFS.range, XSD.float))
                elif campo.tipo == "boolean":
                    g.add((prop_uri, RDFS.range, XSD.boolean))
                elif campo.tipo == "date":
                    g.add((prop_uri, RDFS.range, XSD.dateTime))  # Cambiado de XSD.date a XSD.dateTime
                # Para strings, no se necesita RDFS.range explícito a XSD.string

        except Exception as e:
            print(f"Error al procesar el esquema YAML {yaml_file}: {e}")

    return g 