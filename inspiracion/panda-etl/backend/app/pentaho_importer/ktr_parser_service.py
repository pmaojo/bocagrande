import xml.etree.ElementTree as ET
from typing import Dict, Any, List

from .transformation_model import TransformationModel, Connection, Step, Hop, Field

class KTRParserService:
    """Parses Pentaho KTR files into a TransformationModel."""

    def _parse_text(self, element: ET.Element, tag: str, default: str = '') -> str:
        node = element.find(tag)
        return node.text.strip() if node is not None and node.text is not None else default

    def _parse_int(self, element: ET.Element, tag: str, default: int = 0) -> int:
        node = element.find(tag)
        try:
            return int(node.text) if node is not None and node.text is not None else default
        except (ValueError, TypeError):
            return default

    def _parse_connection(self, conn_element: ET.Element) -> Connection:
        return Connection(
            name=self._parse_text(conn_element, 'name'),
            db_type=self._parse_text(conn_element, 'type'),
            host=self._parse_text(conn_element, 'server'),
            db_name=self._parse_text(conn_element, 'database'),
            port=self._parse_text(conn_element, 'port'),
            user=self._parse_text(conn_element, 'username'),
            password=self._parse_text(conn_element, 'password') # Placeholder for secure handling
        )

    def _parse_step_fields(self, step_element: ET.Element, field_path: str = './/fields/field') -> List[Field]:
        fields = []
        for field_node in step_element.findall(field_path):
            fields.append(Field(
                name=self._parse_text(field_node, 'name'),
                data_type=self._parse_text(field_node, 'type'),
                length=self._parse_int(field_node, 'length', -1),
                precision=self._parse_int(field_node, 'precision', -1)
            ))
        return fields

    def _parse_step_config(self, step_element: ET.Element) -> Dict[str, Any]:
        config: Dict[str, Any] = {}
        for child in step_element:
            if child.tag not in ['name', 'type', 'description', 'distribute', 'copies', 'partitioning', 'GUI', 'fields'] and child.text is not None:
                try:
                    config[child.tag] = int(child.text)
                except ValueError:
                    try:
                        config[child.tag] = float(child.text)
                    except ValueError:
                        config[child.tag] = child.text.strip()
            elif child.tag == 'GUI':
                config['GUI'] = {
                    'xloc': self._parse_int(child, 'xloc'),
                    'yloc': self._parse_int(child, 'yloc'),
                    'draw': self._parse_text(child, 'draw', 'Y') == 'Y'
                }
        return config

    def _parse_step(self, step_element: ET.Element, model: TransformationModel) -> Step:
        step_name = self._parse_text(step_element, 'name')
        step_type = self._parse_text(step_element, 'type')
        config = self._parse_step_config(step_element)

        step = Step(name=step_name, step_type=step_type, config=config)
        step.gui_location = config.get('GUI', {'xloc': 0, 'yloc': 0, 'draw': True})

        # Step-specific parsing
        if step_type == 'TableInput':
            step.connection_name = self._parse_text(step_element, 'connection')
            step.sql = self._parse_text(step_element, 'sql')
            # TableInput fields can be directly under <fields><field> or sometimes within <lookup><field>
            step.fields = self._parse_step_fields(step_element, './fields/field') 
            if not step.fields: # Fallback for older or different KTR structures
                 step.fields = self._parse_step_fields(step_element, './lookup/field')

        elif step_type in ['TableOutput', 'InsertUpdate', 'Update', 'Delete']: # Added Update, Delete
            step.connection_name = self._parse_text(step_element, 'connection')
            step.target_schema = self._parse_text(step_element, 'schema')
            step.target_table = self._parse_text(step_element, 'table')
            # Fields to use for the operation (e.g., for insert/update or lookup keys for delete)
            step.fields = self._parse_step_fields(step_element, './/fields/field') 

        elif step_type == 'SelectValues':
            # Handles <fields><field> for select/rename and <fields><meta> for metadata changes
            step.fields = self._parse_step_fields(step_element, './fields/field') 
            meta_fields = []
            for meta_node in step_element.findall('.//fields/meta/field'):
                 meta_fields.append(Field(
                    name=self._parse_text(meta_node, 'name'),
                    data_type=self._parse_text(meta_node, 'type'),
                    length=self._parse_int(meta_node, 'length', -1),
                    precision=self._parse_int(meta_node, 'precision', -1)
                ))
            # Store meta fields separately or integrate if needed by your logic
            step.config['meta_fields'] = meta_fields 

        elif step_type == 'DBLookup':
            step.connection_name = self._parse_text(step_element, 'connection')
            step.target_schema = self._parse_text(step_element, 'schema')
            step.target_table = self._parse_text(step_element, 'table')
            # Lookup keys, return values
            step.config['keys'] = self._parse_step_fields(step_element, './/keys/key')
            step.config['values'] = self._parse_step_fields(step_element, './/values/value')

        elif step_type == 'StreamLookup':
            # Info step name, lookup keys, return values from stream
            step.config['info_step_name'] = self._parse_text(step_element, './/lookup/from')
            step.config['keys'] = self._parse_step_fields(step_element, './/lookup/key')
            step.config['values'] = self._parse_step_fields(step_element, './/lookup/value')

        elif step_type == 'MergeJoin':
            step.config['join_type'] = self._parse_text(step_element, 'join_type') # LEFT OUTER, INNER, etc.
            step.config['step1'] = self._parse_text(step_element, 'step1') # Name of first input step
            step.config['step2'] = self._parse_text(step_element, 'step2') # Name of second input step
            step.config['keys1'] = [k.text for k in step_element.findall('.//keys1/key') if k.text]
            step.config['keys2'] = [k.text for k in step_element.findall('.//keys2/key') if k.text]
        
        elif step_type == 'Formula':
            formulas = []
            for formula_node in step_element.findall('.//formula'):
                formulas.append({
                    'field_name': self._parse_text(formula_node, 'field_name'),
                    'formula_string': self._parse_text(formula_node, 'formula_string'),
                    'value_type': self._parse_text(formula_node, 'value_type'),
                    'value_length': self._parse_int(formula_node, 'value_length', -1),
                    'value_precision': self._parse_int(formula_node, 'value_precision', -1),
                    'replace_field': self._parse_text(formula_node, 'replace_field')
                })
            step.config['formulas'] = formulas

        # Add more step-specific parsing as needed (e.g., CSVInput, ExcelInput, FixedInput, RegexEval, ScriptValueMod, etc.)

        return step

    def _parse_hop(self, hop_element: ET.Element) -> Hop:
        return Hop(
            from_step=self._parse_text(hop_element, 'from'),
            to_step=self._parse_text(hop_element, 'to'),
            enabled=(self._parse_text(hop_element, 'enabled', 'Y') == 'Y')
        )

    def parse_ktr_file(self, file_path: str) -> TransformationModel:
        """Parses a KTR file and returns a TransformationModel."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
        except ET.ParseError as e:
            print(f"Error parsing XML file {file_path}: {e}")
            # Optionally, raise a custom exception or return an empty/error model
            return TransformationModel(name=f"Error_Parsing_{file_path}")

        model = TransformationModel(
            name=self._parse_text(root, './/info/name'),
            description=self._parse_text(root, './/info/description'),
            directory=self._parse_text(root, './/info/directory_path')
        )

        # Parse connections
        for conn_node in root.findall('.//connection'):
            model.add_connection(self._parse_connection(conn_node))

        # Parse steps
        for step_node in root.findall('.//step'):
            model.add_step(self._parse_step(step_node, model))

        # Parse hops
        for hop_node in root.findall('.//order/hop'):
            model.add_hop(self._parse_hop(hop_node))
            
        # Parse transformation parameters
        for param_node in root.findall('.//parameters/parameter'):
            param_name = self._parse_text(param_node, 'name')
            param_value = self._parse_text(param_node, 'default_value')
            model.parameters[param_name] = param_value
            
        # Parse general transformation attributes/notes (if any structured ones exist)
        # For example, notes are often unstructured text, but some KTRs might have custom attributes.
        # This is a placeholder for more specific attribute parsing if needed.
        notes = root.find('.//info/notepad') 
        if notes is not None:
            model.attributes['notes'] = self._parse_text(notes, 'note')

        return model

if __name__ == '__main__':
    # Example usage (replace with a path to your KTR file)
    # This is for testing the parser directly. 
    # In the application, this service would be called by another part of the backend.
    parser = KTRParserService()
    # Ensure you have a sample KTR file for testing, e.g., 'sample.ktr'
    # For example, if you have a file named 'Vivaldi.kjb' (though this parser is for KTR)
    # or a KTR file in 'pentaho_examples' directory.
    # Adjust the path as necessary.
    # test_ktr_file = '../../pentaho_examples/your_sample.ktr' # Example path
    test_ktr_file = '/Users/pelayo/MagicETL/pentaho_examples/Vivaldi.kjb' # This is a KJB, parser expects KTR
    
    # A better test would be with an actual KTR file.
    # If 'Vivaldi.kjb' is actually a KTR misnamed, it might partially work or fail gracefully.
    # If it's a KJB (job), the structure is different and this KTR parser won't work as intended.
    print(f"Attempting to parse: {test_ktr_file}")
    # model = parser.parse_ktr_file(test_ktr_file)

    # print(f"Parsed Transformation: {model.name}")
    # print(f"Description: {model.description}")
    # print(f"Directory: {model.directory}")
    # print(f"Parameters: {model.parameters}")
    # print(f"Attributes: {model.attributes}")

    # print("\nConnections:")
    # for conn in model.connections:
    #     print(f"  {conn}")

    # print("\nSteps (Execution Order):")
    # for step in model.get_execution_order():
    #      print(f"  {step.name} ({step.type}) - Config: {step.config.get('sql', 'N/A SQL') if step.type == 'TableInput' else step.config}")
    #      if step.fields:
    #          print(f"    Fields: {[f.name for f in step.fields]}")

    # print("\nHops:")
    # for hop in model.hops:
    #     print(f"  {hop}")

    # Example of how to access specific step details
    # if model.get_step_by_name('YourStepName'):
    #    print(f"Details for YourStepName: {model.get_step_by_name('YourStepName').config}")
    pass # Keep the if __name__ block for potential future local testing
