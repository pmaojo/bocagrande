"""
Infraestructura: Generador de shapes SHACL a partir de un TableSchema (YAML).
"""
from ontology.model import TableSchema
from ontology.utils import limpiar_para_uri


def _ttl_string(text: str) -> str:
    """Quote ``text`` as a Turtle string literal (backslashes and quotes escaped)."""
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


# ISO date, optionally followed by the time part OntologyBuilder appends (xsd:dateTime)
_ISO_DATE_RE = r"\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?"
_SLASH_DATE_RE = r"\d{4}/\d{2}/\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?"


def _local(name: str) -> str:
    """Local name used for classes and properties; must match ``OntologyBuilder``."""
    return limpiar_para_uri(str(name).upper())

def generar_shape_shacl(tabla_schema: TableSchema) -> str:
    """
    Genera un shape SHACL (Turtle) para la tabla_schema.
    """
    prefix = """@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix bg:  <http://bocagrande.local/ont#>.

"""
    nombre_tabla = tabla_schema.name
    clase = f"bg:{_local(nombre_tabla)}"  # OntologyBuilder crea la clase en mayúsculas
    shape_name = f"bg:{_local(nombre_tabla)}Shape"
    props = []
    for campo in tabla_schema.fields: # Iterar sobre tabla_schema.fields
        path = f"bg:{_local(campo.name)}"  # OntologyBuilder crea las propiedades en mayúsculas
        tipo = campo.tipo
        if tipo == "integer":
            datatype = "xsd:integer"
        elif tipo == "string":
            datatype = "xsd:string"
        elif tipo == "date":
            datatype = "xsd:dateTime"  # OntologyBuilder writes dates as xsd:dateTime
        elif tipo == "float":
            datatype = "xsd:float"
        elif tipo == "boolean":
            datatype = "xsd:boolean"
        elif tipo == "datetime":
            datatype = "xsd:dateTime"
        else:
            datatype = "xsd:string"  # fallback
        min_count = "1" if getattr(campo, "requerido", False) else "0"
        
        prop_lines = [
            f"        sh:path {path} ;",
            f"        sh:datatype {datatype} ;",
            f"        sh:minCount {min_count} ;"
        ]
        
        if campo.length:
            prop_lines.append(f"        sh:maxLength {campo.length} ;")

        if campo.precision:
            if tipo in {"float", "decimal"}:
                digits_before = max(1, campo.precision - (campo.scale or 0))
                frac = campo.scale or 0
                pattern = rf"^-?\d{{1,{digits_before}}}(?:\.\d{{1,{frac}}})?$"
            else:
                pattern = rf"^-?\d{{1,{campo.precision}}}$"
            prop_lines.append(f"        sh:pattern {_ttl_string(pattern)} ;")

        if campo.formato and tipo == "date":
            # Si el formato es ISO, podemos poner un pattern
            if campo.formato == "%Y-%m-%d":
                prop_lines.append(f"        sh:pattern {_ttl_string('^' + _ISO_DATE_RE + '$')} ;")
            elif campo.formato == "%Y/%m/%d":
                prop_lines.append(f"        sh:pattern {_ttl_string('^' + _SLASH_DATE_RE + '$')} ;")
        
        prop = "\n".join(prop_lines)
        prop = f"    sh:property [\n{prop}\n    ] ;"
        props.append(prop)
    props_str = "\n".join(props)
    shape = f"{shape_name} a sh:NodeShape ;\n    sh:targetClass {clase} ;\n{props_str}\n.\n"
    return prefix + shape 
