import yaml
import csv
from pathlib import Path
import os

def cargar_yamls(directorio):
    yamls = {}
    for archivo in os.listdir(directorio):
        if archivo.endswith('.yaml') and archivo != 'metadatos_tablas.yaml':
            nombre_tabla = archivo.replace('.yaml', '')
            with open(os.path.join(directorio, archivo), 'r', encoding='utf-8') as f:
                yamls[nombre_tabla] = yaml.safe_load(f)
    return yamls

def exportar_a_neo4j_csv(yamls, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    # 1. Nodos de tabla
    with open(os.path.join(output_dir, 'tablas.csv'), 'w', newline='', encoding='utf-8') as f_tablas:
        writer = csv.DictWriter(f_tablas, fieldnames=['tabla', 'descripcion'])
        writer.writeheader()
        for tabla, campos in yamls.items():
            # Descripción: concatena descripciones de campos principales
            desc = ', '.join([c.get('Descripción','') for c in campos[:3] if c.get('Descripción')])
            writer.writerow({'tabla': tabla, 'descripcion': desc})
    # 2. Nodos de campo
    with open(os.path.join(output_dir, 'campos.csv'), 'w', newline='', encoding='utf-8') as f_campos:
        fieldnames = ['campo', 'tipo', 'longitud', 'enum', 'deprecado', 'formato', 'foreign_key', 'tabla']
        writer = csv.DictWriter(f_campos, fieldnames=fieldnames)
        writer.writeheader()
        for tabla, campos in yamls.items():
            for c in campos:
                writer.writerow({
                    'campo': c.get('Campo',''),
                    'tipo': c.get('Tipo',''),
                    'longitud': c.get('Longitud',''),
                    'enum': '|'.join(c['Enum']) if 'Enum' in c else '',
                    'deprecado': c.get('Deprecado', False),
                    'formato': c.get('Formato',''),
                    'foreign_key': c.get('foreign_key',''),
                    'tabla': tabla
                })
    # 3. Relaciones
    with open(os.path.join(output_dir, 'relaciones.csv'), 'w', newline='', encoding='utf-8') as f_rel:
        writer = csv.DictWriter(f_rel, fieldnames=['from','to','tipo'])
        writer.writeheader()
        for tabla, campos in yamls.items():
            for c in campos:
                # HAS_FIELD
                writer.writerow({'from': tabla, 'to': c.get('Campo',''), 'tipo': 'HAS_FIELD'})
                # FOREIGN_KEY
                if c.get('foreign_key'):
                    ref = c['foreign_key'].split('.')[0]
                    writer.writerow({'from': c.get('Campo',''), 'to': ref, 'tipo': 'FOREIGN_KEY'})

def main():
    yamls = cargar_yamls('data/output_yaml')
    exportar_a_neo4j_csv(yamls, 'data/neo4j_import')

if __name__ == '__main__':
    main() 