"""Utility helpers shared by ontology builders."""
from urllib.parse import quote
from rdflib import Namespace

# Base namespace for all ontology elements
BASE = Namespace("http://bocagrande.local/ont#")


def limpiar_para_uri(texto: str) -> str:
    """Sanitize text for use in a URI fragment."""
    return quote(str(texto).split()[0].replace(" ", "_"), safe="")

__all__ = ["BASE", "limpiar_para_uri"]

