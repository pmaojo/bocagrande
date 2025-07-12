"""
Infraestructura: Generador de shapes SHACL a partir de un TableSchema (YAML).
"""
from ontology.model import TableSchema
from typing import Any

def generar_shape_shacl(tabla_schema: TableSchema) -> str:
    """
    Genera un shape SHACL (Turtle) para la tabla_schema.
    """
    prefix = """@prefix sh: <http://www.w3.org/ns/shacl#>.
@prefix xsd: <http://www.w3.org/2001/XMLSchema#>.
@prefix bg:  <http://bocagrande.local/ont#>.

"""
    nombre_tabla = tabla_schema.name
    clase = f"bg:{nombre_tabla}"  # Asumimos que la clase OWL es bg:Tabla
    shape_name = f"bg:{nombre_tabla}Shape"
    props = []
    for campo in tabla_schema.fields: # Iterar sobre tabla_schema.fields
        path = f"bg:{campo.name}"
        tipo = campo.tipo
        if tipo == "integer":
            datatype = "xsd:integer"
        elif tipo == "string":
            datatype = "xsd:string"
        elif tipo == "date":
            datatype = "xsd:date"
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

        if campo.formato and tipo == "date":
            # Si el formato es ISO, podemos poner un pattern
            if campo.formato == "%Y-%m-%d":
                prop_lines.append('        sh:pattern "^\\d{4}-\\d{2}-\\d{2}$" ;')
            elif campo.formato == "%Y/%m/%d":
                prop_lines.append('        sh:pattern "^\\d{4}/\\d{2}/\\d{2}$" ;')
        
        prop = "\n".join(prop_lines)
        prop = f"    sh:property [\n{prop}\n    ] ;"
        props.append(prop)
    props_str = "\n".join(props)
    shape = f"{shape_name} a sh:NodeShape ;\n    sh:targetClass {clase} ;\n{props_str}\n.\n"
    return prefix + shape 