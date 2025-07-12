from abc import ABC, abstractmethod
from typing import List, Dict, Any

class SchemaNormalizer(ABC):
    @abstractmethod
    def normalize(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        ...

def validate_schema(schema: Dict[str, Any]) -> List[str]:
    """
    Valida el esquema extraído: tipos, longitudes, unicidad, claves, required, etc.
    Devuelve una lista de errores encontrados.
    """
    errores = []
    tipos_validos = {"int", "string", "float", "date", "bool"}
    for pagina in schema.get("paginas", []):
        for tabla in pagina.get("tables", []):
            nombres = set()
            for campo in tabla.get("rows", []):
                nombre = campo.get("name", "").strip()
                tipo = campo.get("type", "").strip().lower()
                if not nombre:
                    errores.append(f"Campo sin nombre en página {pagina.get('number')} tabla")
                    continue
                if nombre in nombres:
                    errores.append(f"Campo duplicado '{nombre}' en página {pagina.get('number')} tabla")
                nombres.add(nombre)
                if tipo not in tipos_validos:
                    errores.append(f"Tipo inválido '{tipo}' en campo '{nombre}' de página {pagina.get('number')}")
                if "required" not in campo:
                    errores.append(f"Campo '{nombre}' sin atributo 'required' en página {pagina.get('number')}")
                if "length" in campo and campo["length"]:
                    try:
                        int(str(campo["length"]))
                    except Exception:
                        errores.append(f"length inválido en campo '{nombre}' de página {pagina.get('number')}")
    return errores 