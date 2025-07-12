#!/usr/bin/env python3
"""Utility to verify that YAML schemas load correctly."""
import yaml
from pathlib import Path

def test_yaml_parsing():
    """Test that YAML files can be parsed correctly with the new structure."""
    schema_dir = Path("schema_yaml")
    
    # Test a few YAML files to make sure they can be parsed
    for yaml_file in schema_dir.glob("*.yaml"):
        if "metadatos" in yaml_file.name.lower():
            continue  # Skip metadata files
            
        print(f"Testing {yaml_file.name}...")
        try:
            with open(yaml_file, "r") as f:
                schema = yaml.safe_load(f)
            
            # Check if it's a list structure (the correct format)
            if isinstance(schema, list):
                print(f"  ✓ {yaml_file.name} is a list with {len(schema)} fields")
                # Check if it has the expected "Campo" structure
                if schema and isinstance(schema[0], dict) and "Campo" in schema[0]:
                    print(f"  ✓ {yaml_file.name} has correct 'Campo' structure")
                    # Count how many fields have "Campo" keys
                    campo_fields = [field for field in schema if isinstance(field, dict) and "Campo" in field]
                    print(f"  ✓ Found {len(campo_fields)} fields with 'Campo' keys")
                else:
                    print(f"  ✗ {yaml_file.name} doesn't have expected 'Campo' structure")
            else:
                print(f"  ✗ {yaml_file.name} is not a list structure")
                
        except Exception as e:
            print(f"  ✗ Error parsing {yaml_file.name}: {e}")
        
        print()

if __name__ == "__main__":
    test_yaml_parsing()