"""Utility helpers shared by ontology builders."""
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote
from rdflib import Namespace

# Base namespace for all ontology elements
BASE = Namespace("http://bocagrande.local/ont#")


def limpiar_para_uri(texto: str) -> str:
    """Sanitize text for use in a URI fragment."""
    return quote(str(texto).split()[0].replace(" ", "_"), safe="")

def a_xsd_datetime(valor: Any, formato: Optional[str] = None) -> str:
    """Lexical form of an ``xsd:dateTime`` for ``valor``.

    Parses with the schema's ``formato`` (a ``strptime`` pattern) when given. A value that does not
    match is returned as written, so SHACL reports it instead of it being silently altered.
    Date-only ISO values get ``T00:00:00``; datetime objects are written with ``isoformat``.
    """
    if hasattr(valor, "isoformat"):
        return valor.isoformat()
    texto = str(valor).strip()
    if formato:
        try:
            return datetime.strptime(texto, formato).isoformat()
        except ValueError:
            pass
    if len(texto) == 10 and texto.count("-") == 2:
        return texto + "T00:00:00"
    return texto


__all__ = ["BASE", "limpiar_para_uri", "a_xsd_datetime"]

