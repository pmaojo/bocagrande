from ruamel.yaml import YAML
from typing import Any, Dict

def serialize_yaml(schema: Dict[str, Any], output_path: str) -> None:
    """
    Serializa el esquema limpio a YAML, respetando orden y unicode.
    """
    yaml = YAML()
    yaml.allow_unicode = True
    yaml.default_flow_style = False
    with open(output_path, 'w', encoding='utf-8') as f:
        yaml.dump(schema, f) 