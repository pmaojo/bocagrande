"""
Aplicación: servicio para construir la ontología OWL a partir de esquemas y datos.
"""
from typing import Any, Tuple, Optional
import tempfile
import os

from adapter.reasoner import Reasoner
from rdflib import Graph, RDF, RDFS, OWL, Literal, URIRef, XSD
import pandas as pd

from .utils import BASE, limpiar_para_uri

class OntologyBuilder:
    """
    Orquesta la construcción de un grafo OWL (rdflib.Graph) a partir de TableSchema y datos (DataFrame).
    Ahora, recibe una TBox base y solo añade los individuos (ABox).
    """
    def __init__(self, tbox_base_graph: Graph, reasoner: Optional[Reasoner] = None):
        self.tbox_base_graph = tbox_base_graph
        self.reasoner = reasoner

    def build_abox_graph(self, schema, df) -> Graph:
        """
        Devuelve un grafo OWL con las instancias (ABox) añadidas a la TBox base.
        """
        g = Graph() # Crear un nuevo grafo
        g += self.tbox_base_graph # Añadir las triples de la TBox base al nuevo grafo
        # No es necesario volver a bindear, la adición de grafos mantiene los bindings

        class_uri = BASE[limpiar_para_uri(schema.name.upper())]

        # Instancias
        for idx, row in df.iterrows():
            # Usamos el primary_key si está definido, si no, generamos un URI simple
            if schema.primary_key:
                pk_values = "-".join(str(row[k]) for k in schema.primary_key if k in row)
                if pk_values:
                    ind_uri = BASE[f"{limpiar_para_uri(schema.name)}_{limpiar_para_uri(pk_values)}"]
                else:
                    ind_uri = BASE[f"{limpiar_para_uri(schema.name)}_{idx+1}"] # Fallback si PK está vacía
            else:
                ind_uri = BASE[f"{limpiar_para_uri(schema.name)}_{idx+1}"]

            g.add((ind_uri, RDF.type, class_uri))

            for field in schema.fields:
                if field.name in row and pd.notna(row[field.name]): # Evitar NaNs
                    prop_uri = BASE[limpiar_para_uri(field.name.upper())]
                    valor = row[field.name]

                    # Mapeo de tipos de datos para literales RDF
                    if field.tipo == "integer":
                        literal_valor = Literal(valor, datatype=XSD.integer)
                    elif field.tipo == "float":
                        literal_valor = Literal(valor, datatype=XSD.float)
                    elif field.tipo == "boolean":
                        literal_valor = Literal(valor, datatype=XSD.boolean)
                    elif field.tipo == "date":
                        # Forzar a xsd:dateTime (añadir T00:00:00 si no tiene parte de tiempo)
                        try:
                            str_val = str(valor)
                            if len(str_val) == 10 and str_val.count("-") == 2:
                                str_val += "T00:00:00"
                            literal_valor = Literal(str_val, datatype=XSD.dateTime)
                        except Exception:
                            literal_valor = Literal(str(valor))
                    else:
                        literal_valor = Literal(valor) # Default a string
                    g.add((ind_uri, prop_uri, literal_valor))
        return g

    def reason_graph(self, graph: Graph) -> Tuple[bool, str]:
        """Run the configured reasoner on the provided graph."""
        if self.reasoner is None:
            raise ValueError("No reasoner configured")
        tmp_path = ""
        with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False) as tmp:
            tmp_path = tmp.name
            graph.serialize(destination=tmp_path, format="turtle")

        try:
            return self.reasoner.reason(tmp_path)
        finally:
            os.unlink(tmp_path)
